#!/usr/bin/env python3
"""
Sync active Canton validators into Attio (Validators object).

- Fetches validator licenses from https://api.cantonnodes.com/v0/admin/validator/licenses
- Follows pagination using `next_page_token` / `after`
- Filters to validators active in the last 7 days (payload.lastActiveAt)
- Upserts into Attio using PUT /v2/objects/{object}/records?matching_attribute=id

Attio fields used:
- id              (slug: "id")              ← unique validator ID from Canton: payload.validator
- sponsor         (slug: "sponsor")         ← sponsor from Canton: payload.sponsor
- contact_email_1 (slug: "contact_email_1") ← contactPoint from Canton: payload.metadata.contactPoint
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone

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

# Optional tiny delay between Attio calls
ATTIO_REQUEST_DELAY_SECONDS = 0.05  # 50 ms


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
        "Filtered to %d active validators in the last %d days.",
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
    Upsert one validator into Attio using the Assert-record style endpoint:
    PUT /v2/objects/{object}/records?matching_attribute=id

    Keys in data.values are attribute slugs (id, sponsor, contact_email_1).
    """
    payload = license_obj.get("payload") or {}
    metadata = payload.get("metadata") or {}

    validator_id = payload.get("validator")
    sponsor = payload.get("sponsor")
    contact_point = metadata.get("contactPoint")

    if not validator_id:
        return

    # Build the values payload using attribute slugs
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

    body = {
        "data": {
            "values": values
        }
    }

    response = requests.put(url, headers=get_attio_headers(), params=params, json=body, timeout=30)

    if not response.ok:
        logging.error(
            "Failed to upsert validator=%s into Attio (HTTP %s): %s",
            validator_id,
            response.status_code,
            response.text,
        )
    else:
        logging.debug("Upserted validator=%s into Attio.", validator_id)

    time.sleep(ATTIO_REQUEST_DELAY_SECONDS)


# ------------- Main entrypoint -------------

def main():
    logging.info("Starting Canton Scan → Attio Validators sync...")

    try:
        licenses = fetch_all_validator_licenses()
        active_licenses = filter_active_licenses(licenses, lookback_days=ACTIVE_LOOKBACK_DAYS)

        for lic in active_licenses:
            upsert_validator_into_attio(lic)

        logging.info(
            "Completed sync of %d active validators into Attio.",
            len(active_licenses),
        )

    except Exception as exc:
        logging.exception("Sync failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
