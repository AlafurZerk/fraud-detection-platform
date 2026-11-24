from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.routers import health, alerts, analytics

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Production REST API Serving Layer for Real-Time Fraud Alerts & Batch Analytical Risk Profiles.",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(alerts.router)
app.include_router(analytics.router)