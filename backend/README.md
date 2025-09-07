This is the backend for the nba-analytics-app. It's a small FastAPI service that provides HTTP APIs consumed by the frontend.

Prerequisites
-------------
- Python 3.11+ (recommended)
- pip
- (optional) virtual environment tool (venv)

PowerShell
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run (development)
PowerShell
```
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Docs:
http://127.0.0.1:8000/docs

Run (production)
----------------
Use a production ASGI server and configure workers appropriately. Example (simple):

PowerShell
```
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

Running Tests
----------------
PowerShell
```
pytest --cov=app --cov-report=term-missing --cov-report=html
```