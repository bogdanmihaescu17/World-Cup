# World Cup Predictor App

Flask + PostgreSQL app with:

- `admin` role: manage users, import Excel data, set official match scores, export ranking
- `user` role: submit predictions before kickoff, view own predictions, view global ranking
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

## 4) First login

Default admin credentials come from `.env`:

- username: `ADMIN_USERNAME` (default `admin`)
- password: `ADMIN_PASSWORD` (default `admin123`)

Change these immediately after first run.

## 5) Import Excel

From admin panel click **Import from Excel**.

The app reads:

- `Matches` sheet for fixtures and kickoff data
- `Predictions_Ranking_1` and `Predictions_Ranking_2` for initial users

Imported users get password from `DEFAULT_IMPORTED_USER_PASSWORD`.
