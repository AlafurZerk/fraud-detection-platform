from datetime import datetime, timezone
from fastapi import APIRouter
from app.database import get_db_cursor
from app.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
def health_check():
    db_ok = False
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1;")
            db_ok = True
    except Exception:
        db_ok = False

    return HealthResponse(
        status="healthy" if db_ok else "unhealthy",
        database_connected=db_ok,
        timestamp=datetime.now(timezone.utc),
    )