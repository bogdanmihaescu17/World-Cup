# World Cup Predictor App

Flask + PostgreSQL app with:

- `admin` role: manage users, import Excel data, set official match scores, export ranking
- `user` role: submit predictions before kickoff, view own predictions, view global ranking
- **Group stage** (`/group-stage`): group matches only, **group tables** from official results, and **prediction ranking** counting group-stage matches only
- Scoring rules:
  - exact score = 3 points
  - correct outcome (win/draw/loss), not exact = 1 point
  - otherwise = 0 points
- Ranking updates only when admin sets official result

## 1) Setup

Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

## 2) Run locally (Python venv)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python app.py
```

App URL: `http://localhost:5001`

## 3) Run with Docker

```bash
docker compose up --build
```

App URL: `http://localhost:5001`

## 4) Deploy on Railway

Railway detects `Procfile` automatically.

Start command:

```bash
gunicorn --bind 0.0.0.0:$PORT app:app
```

Set these Railway variables:

- `DATABASE_URL` = your Railway Postgres URL
- `SECRET_KEY` = strong random string
- `ADMIN_USERNAME` = admin username
- `ADMIN_PASSWORD` = admin password
- `EXCEL_FILE_PATH` = `World Cup_2026.xlsx`

Optional:

- `AUTO_INIT_DB=true` (default)
- `AUTO_IMPORT_EXCEL=true` (default; imports matches/users if DB has zero matches)
- `DEFAULT_IMPORTED_USER_PASSWORD=changeme123`
- `LOG_LEVEL=INFO`

Health check endpoint:

- `GET /health` returns `200` with JSON status

## 5) First login

Default admin credentials come from `.env`:

- username: `ADMIN_USERNAME` (default `admin`)
- password: `ADMIN_PASSWORD` (default `admin123`)

Change these immediately after first run.

## 6) Import Excel

From admin panel click **Import from Excel**.

The app reads:

- `Matches` sheet for fixtures and kickoff data
- `Predictions_Ranking_1` and `Predictions_Ranking_2` for initial users

Imported users get password from `DEFAULT_IMPORTED_USER_PASSWORD`.
