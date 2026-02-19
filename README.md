# Canton → Attio Validator Sync

Syncs active Canton network validators into an Attio CRM object, running automatically every 24 hours via GitHub Actions.

## What it does

1. Fetches all validator licenses from the Canton Scan API (paginated)
2. Filters to validators active in the last 7 days (`payload.lastActiveAt`)
3. Deduplicates by validator ID
4. Upserts each validator into Attio using concurrent writes (8 threads)

Fields synced per validator:
- `id` — validator ID (unique matching attribute)
- `sponsor` — sponsoring entity
- `contact_email_1` — contact point from validator metadata

## Schedule

Runs daily at **03:00 UTC** via GitHub Actions. Can also be triggered manually from the Actions tab.

## Setup

### 1. Attio API token

Add your Attio API token as a GitHub Actions secret named `ATTIO_API_TOKEN`:

```
GitHub repo → Settings → Secrets and variables → Actions → New repository secret
Name: ATTIO_API_TOKEN
Value: <your token>
```

### 2. Attio object configuration

The target Attio object and attribute slugs are configured at the top of `sync_canton_validators_to_attio.py`:

```python
ATTIO_VALIDATORS_OBJECT_ID = "cb097981-2137-47a2-9ddf-0b1d88d3c372"
ATTIO_ATTR_VALIDATOR       = "id"
ATTIO_ATTR_SPONSOR         = "sponsor"
ATTIO_ATTR_CONTACT_POINT   = "contact_email_1"
```

Update these if your Attio object ID or attribute slugs differ.

### 3. Running locally

```bash
pip install requests
export ATTIO_API_TOKEN=your_token_here
python sync_canton_validators_to_attio.py
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ACTIVE_LOOKBACK_DAYS` | `7` | Days back to consider a validator "active" |
| `ATTIO_MAX_WORKERS` | `8` | Concurrent Attio write threads |
| `ATTIO_REQUEST_DELAY_SECONDS` | `0.1` | Base delay for retry backoff |

## Error handling

- Retries up to 4 times on network errors, 429 (rate limit), and 5xx responses
- Exponential-style backoff between retries
- Non-retryable errors (4xx excluding 429) are logged and skipped
- Exit code 1 on fatal errors (caught by GitHub Actions)
