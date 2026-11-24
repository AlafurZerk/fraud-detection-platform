from typing import List
from fastapi import APIRouter, Query
from app.database import get_db_cursor
from app.schemas import FraudAlertResponse, AlertsSummaryStats

router = APIRouter(prefix="/api/v1/alerts", tags=["Speed Layer (Real-time Alerts)"])


@router.get("/recent", response_model=List[FraudAlertResponse])
def get_recent_alerts(limit: int = Query(default=20, ge=1, le=100)):
    """Возвращает последние N инцидентов из стриминга"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT id, transaction_id, user_id, amount, merchant_category, 
                   alert_type, event_time, detected_at, created_at
            FROM fraud_alerts
            ORDER BY id DESC
            LIMIT %s;
            """,
            (limit,)
        )
        rows = cursor.fetchall()
        return [FraudAlertResponse(**dict(row)) for row in rows]


@router.get("/stats", response_model=AlertsSummaryStats)
def alerts_stats():

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                COUNT(*) as total_alerts,
                COALESCE(SUM(amount), 0) as total_volume,
                COALESCE(AVG(amount), 0) as avg_amount,
                COALESCE(MAX(amount), 0) as max_amount
            FROM fraud_alerts;
            """
        )
        row = cursor.fetchone()
        return AlertsSummaryStats(**dict(row))