from fastapi import FastAPI
from app.api import routes

app = FastAPI(
    title="NBA Analytics API",
    description="Backend for NBA natural language stats queries",
    version="0.1.0"
)

app.include_router(routes.router, prefix="/api")


@app.get("/")
def root():
    return {"message": "Welcome to the NBA Analytics API"}
