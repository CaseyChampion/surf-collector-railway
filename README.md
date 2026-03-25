# Del Mar Surf Collector (Railway Cron Job)

This repo runs `collect_data.py` as a Railway scheduled job and writes rows into `surf_observations` in Supabase.

## Files
- `collect_data.py` — fetches swell, buoy, wind, and tide data, then inserts one row into Supabase
- `requirements.txt` — Python dependency list
- `.env.example` — environment variables to copy into Railway Variables
- `railway.toml` — tells Railway to run `python collect_data.py`

## What changed from your local script
- Removed hardcoded Supabase credentials
- Reads config from environment variables
- Logs cleanly to stdout for Railway
- Stores both UTC and Los Angeles timestamps
- Persists partial fetch errors in the inserted row when possible
- Exits cleanly, which is what Railway cron jobs expect

## Local test
```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
# then fill in the real values
export SUPABASE_URL=...
export SUPABASE_KEY=...
python collect_data.py
```

## Railway deploy
1. Push this folder to a new GitHub repo.
2. In Railway, create a **New Service** from that GitHub repo.
3. In the service settings, set the **Start Command** to `python collect_data.py` if Railway does not pick it up automatically.
4. Add the variables from `.env.example` in Railway's Variables tab.
5. In **Settings → Cron Schedule**, set:
   ```
   */30 * * * *
   ```
6. Trigger one manual deploy and inspect logs.
7. Confirm rows are showing up in `surf_observations`.

## Notes
- Railway cron jobs are intended for scheduled tasks that run and exit on completion.
- If Railway cannot infer a Python start command, Railway docs say to set one explicitly in service settings.
- Scheduled services should finish and exit promptly rather than loop forever.
