#!/usr/bin/env python3
"""
Sync active Canton validators into Attio (Validators object).

- Fetches validator licenses from https://api.cantonnodes.com/v0/admin/validator/licenses
- Follows pagination using `next_page_token` / `after`
- Filters to validators active in the last 7 days (payload.lastActiveAt)
- Deduplicates by validator ID
- Upserts into Attio using PUT /v2/objects/{object}/records?matching_attribute=id
- Uses a thread pool for concurrent Attio writes
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ------------- Configuration -------------

# Canton Scan API base
CANTON_SCAN_BASE_URL = "https://api.cantonnodes.com"

# How many days back counts as “active”
ACTIVE_LOOKBACK_DAYS = 7

# Attio API base
ATTIO_API_BASE_URL = "https://api.attio.com/v2"

# Your Attio Validators object ID
ATTIO_VALIDATORS_OBJECT_ID = "cb097981-2137-47a2-9ddf-0b1d88d3c372"

# Attribute slugs in Attio for the Validator object
ATTIO_ATTR_VALIDATOR = "id"               # unique attribute, used as matching_attribute
ATTIO_ATTR_SPONSOR = "sponsor"
ATTIO_ATTR_CONTACT_POINT = "contact_email_1"

# Name of the environment variable that holds the Attio API token
ATTIO_TOKEN_ENV_VAR = "ATTIO_API_TOKEN"

# Base delay used for backoff between retries (seconds)
ATTIO_REQUEST_DELAY_SECONDS = 0.1

# Number of concurrent Attio write requests.
# 8 workers keeps us well below Attio's 25 writes/sec limit in practice.
ATTIO_MAX_WORKERS = 8


# ------------- Logging -------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ------------- Canton Scan helpers -------------

def fetch_all_validator_licenses():
    """
    Fetch all validator licenses from Canton Scan, following pagination.
    Uses /v0/admin/validator/licenses with `after` for paging.
    """
    all_licenses = []
    after = None

    while True:
        params = {}
        if after is not None:
            params["after"] = after

        url = f"{CANTON_SCAN_BASE_URL}/v0/admin/validator/licenses"
        logging.info("Fetching validator licenses (after=%s)...", after)

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        page_licenses = data.get("validator_licenses") or []
        all_licenses.extend(page_licenses)

        after = data.get("next_page_token")
        if not after:
            break

    logging.info("Fetched %d total validator licenses from Canton Scan.", len(all_licenses))
    return all_licenses


def filter_active_licenses(licenses, lookback_days=ACTIVE_LOOKBACK_DAYS):
    """
    Filter validator licenses to those active within the last `lookback_days`
    based on payload.lastActiveAt (ISO8601 timestamps).
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)

    active = []
    for lic in licenses:
        payload = lic.get("payload") or {}
        last_active_at = payload.get("lastActiveAt")
        if not last_active_at:
            continue

        try:
            # Convert "2025-01-23T12:48:21.220193Z" to timezone-aware datetime
            last_active = datetime.fromisoformat(last_active_at.replace("Z", "+00:00"))
        except ValueError:
            logging.warning(
                "Could not parse lastActiveAt=%r for validator=%r",
                last_active_at,
                payload.get("validator"),
            )
            continue

        if last_active >= cutoff:
            active.append(lic)

    logging.info(
        "Filtered to %d active validator licenses in the last %d days.",
        len(active),
        lookback_days,
    )
    return active


# ------------- Attio helpers -------------

def get_attio_headers():
    token = os.environ.get(ATTIO_TOKEN_ENV_VAR)
    if not token:
        logging.error("Environment variable %s is not set.", ATTIO_TOKEN_ENV_VAR)
        sys.exit(1)

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def upsert_validator_into_attio(license_obj):
    """
    Upsert one validator into Attio using:
    PUT /v2/objects/{object}/records?matching_attribute=id

    Returns True on success, False on permanent failure.
    """
    payload = license_obj.get("payload") or {}
    metadata = payload.get("metadata") or {}

    validator_id = payload.get("validator")
    sponsor = payload.get("sponsor")
    contact_point = metadata.get("contactPoint")

    if not validator_id:
        return False

    values = {
        ATTIO_ATTR_VALIDATOR: validator_id
    }
    if sponsor is not None:
        values[ATTIO_ATTR_SPONSOR] = sponsor
    if contact_point is not None:
        values[ATTIO_ATTR_CONTACT_POINT] = contact_point

    url = f"{ATTIO_API_BASE_URL}/objects/{ATTIO_VALIDATORS_OBJECT_ID}/records"
    params = {
        "matching_attribute": ATTIO_ATTR_VALIDATOR
    }

    max_attempts = 4

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.put(
                url,
                headers=get_attio_headers(),
                params=params,
                json={"data": {"values": values}},
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            logging.warning(
                "Network error talking to Attio for validator=%s (attempt %d/%d): %s",
                validator_id,
                attempt,
                max_attempts,
                e,
            )
            response = None

        # Decide whether to retry or stop based on the response
        if response is None:
            # network error → retry unless out of attempts
            should_retry = True
            sleep_seconds = ATTIO_REQUEST_DELAY_SECONDS * attempt
        else:
            status = response.status_code

            if status == 429:
                # Rate limit exceeded. Attio docs say next second usually. :contentReference[oaicite:2]{index=2}
                logging.warning(
                    "Rate limit (429) from Attio for validator=%s (attempt %d/%d).",
                    validator_id,
                    attempt,
                    max_attempts,
                )
                should_retry = True
                sleep_seconds = max(1.0, ATTIO_REQUEST_DELAY_SECONDS * attempt)
            elif 500 <= status < 600:
                logging.warning(
                    "Transient 5xx error HTTP %s for validator=%s (attempt %d/%d).",
                    status,
                    validator_id,
                    attempt,
                    max_attempts,
                )
                should_retry = True
                sleep_seconds = ATTIO_REQUEST_DELAY_SECONDS * attempt
            elif not response.ok:
                # Non-retryable error: log and stop
                logging.error(
                    "Non-retryable Attio error HTTP %s for validator=%s: %s",
                    status,
                    validator_id,
                    response.text,
                )
                return False
            else:
                # Success
                if attempt > 1:
                    logging.info(
                        "Succeeded for validator=%s after %d attempts.",
                        validator_id,
                        attempt,
                    )
                return True

        if not should_retry or attempt == max_attempts:
            logging.error(
                "Giving up on validator=%s after %d attempts.",
                validator_id,
                max_attempts,
            )
            return False

        logging.info(
            "Retrying validator=%s in %.2fs (attempt %d/%d)...",
            validator_id,
            sleep_seconds,
            attempt + 1,
            max_attempts,
        )
        time.sleep(sleep_seconds)

    return False


# ------------- Main entrypoint -------------

def main():
    logging.info("Starting Canton Scan → Attio Validators sync...")

    try:
        licenses = fetch_all_validator_licenses()
        active_licenses = filter_active_licenses(
            licenses,
            lookback_days=ACTIVE_LOOKBACK_DAYS,
        )

        # Deduplicate by validator ID
        unique_by_validator = {}
        for lic in active_licenses:
            payload = lic.get("payload") or {}
            vid = payload.get("validator")
            if not vid:
                continue
            unique_by_validator[vid] = lic

        unique_licenses = list(unique_by_validator.values())
        logging.info(
            "After de-duplication, %d unique validators will be synced (from %d active licenses).",
            len(unique_licenses),
            len(active_licenses),
        )

        success_count = 0
        fail_count = 0

        # Run Attio upserts concurrently
        with ThreadPoolExecutor(max_workers=ATTIO_MAX_WORKERS) as executor:
            futures = [executor.submit(upsert_validator_into_attio, lic)
                       for lic in unique_licenses]

            for future in as_completed(futures):
                try:
                    if future.result():
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as exc:
                    logging.exception("Unexpected exception in worker: %s", exc)
                    fail_count += 1

        logging.info(
            "Completed sync. %d validators upserted successfully, %d failed.",
            success_count,
            fail_count,
        )

    except Exception as exc:
        logging.exception("Sync failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
