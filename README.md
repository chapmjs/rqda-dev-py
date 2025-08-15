# Mini QDA — Phase 0 (Shiny for Python + MySQL)

A minimal spike to prove: upload plaintext, select text, assign a code, save to MySQL, and re-highlight on reload.

## 1) Configure MySQL

Create a database (example: `miniqda`) and a user with privileges. The app expects these environment variables:

- `DB_HOST` (e.g. `mexico.bbfarm.org`)
- `DB_PORT` (default `3306`)
- `DB_USER`
- `DB_PASS`
- `DB_NAME` (e.g. `miniqda`)

## 2) Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# copy and edit local env (for dev only)
cp .env.example .env
export $(grep -v '^#' .env | xargs)

python app.py  # or: shiny run --reload app.py
```

Then open http://127.0.0.1:8000 (Shiny will print the URL).

## 3) Deploy to Posit Connect Cloud

- Add **Secret variables** on Connect Cloud (`DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME`, optionally `DB_PORT`).
- Install and configure `rsconnect-python` locally:

```bash
pip install rsconnect-python
rsconnect add --server https://connect.posit.cloud --api-key <YOUR-API-KEY> --name cloud
rsconnect deploy shiny -n cloud . --title miniqda-phase0
```

Further docs:
- Shiny for Python on Posit Connect
- Connect Cloud secret variables
- rsconnect-python CLI deploy

## Notes

- Selection anchoring is naive (`indexOf` on `innerText`); good enough for Phase 0.
- Only `.txt` files are treated as plaintext in this spike.
