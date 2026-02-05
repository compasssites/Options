# Option Chain Hub

FastAPI + PWA dashboard for MCX option chain snapshots.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open: `http://localhost:8000`

## Configuration

Edit `config.json` to add symbols and expiries. Example:

```json
{
  "symbols": {
    "SILVERM": {"source": "mcx", "expiries": ["18FEB2026", "25FEB2026"]},
    "GOLDM": {"source": "mcx", "expiries": ["05FEB2026"]},
    "NIFTY": {"source": "nse", "expiries": []}
  }
}
```

If `expiry` is not provided, the app uses the first configured expiry for the symbol.

## Environment variables

- `APP_TOKEN`: Optional. If set, API endpoints require this token (query param `token` or header `X-API-Token`).
- `CACHE_TTL_SECONDS`: Cache TTL in seconds (default 600).
- `DEFAULT_STRIKE_STEP`: Default strike step for filtering (default 5000).
- `CONFIG_PATH`: Path to config file (default `config.json`).
- `TE_API_KEY`: Trading Economics API key (e.g. `guest:guest`) to enable the live gold/silver ticker.
- `TE_CACHE_TTL_SECONDS`: Ticker cache TTL in seconds (default 60).

## CapRover deploy

1. Create a new app in CapRover.
2. Use “Deploy from Git” or upload this folder.
3. Set any env vars in CapRover (optional).

## API

- `GET /api/symbols`
- `GET /api/expiries?symbol=SILVERM`
- `GET /api/option-chain?symbol=SILVERM&expiry=18FEB2026&format=csv`
- `POST /api/refresh?symbol=SILVERM&expiry=18FEB2026`
- `GET /api/option-chain-pretty?symbol=SILVERM&expiry=18FEB2026`
- `GET /api/option-chain-chat?symbol=SILVERM&expiry=18FEB2026` (defaults to NDJSON)
- `GET /api/option-chain?symbol=SILVERM&expiry=18FEB2026&format=lines` (line-numbered text)
- `GET /api/option-chain?symbol=SILVERM&expiry=18FEB2026&format=text` (pretty JSON as text/plain)
- `GET /api/option-chain?symbol=SILVERM&expiry=18FEB2026&pretty=1&as_text=1` (pretty JSON as text/plain)

## Notes

NIFTY uses NSE’s NextApi option chain endpoint. If NSE blocks your server’s IP, the expiry list may appear empty until access is allowed. You can set a default strike for expiry discovery with `NSE_DEFAULT_STRIKE`.
