from typing import List
from fastapi import APIRouter, HTTPException, Path
from app.database import get_db_cursor
from app.schemas import MerchantRiskResponse

router = APIRouter(prefix="/api/v1/analytics", tags=["Batch Layer (Risk Analytics)"])


@router.get("/merchant-risks", response_model=List[MerchantRiskResponse])
def get_all_merchant_risks():

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT merchant_category, total_alerts, total_fraud_volume, 
                   avg_fraud_amount, max_fraud_amount, risk_score, 
                   risk_level, last_updated_at
            FROM merchant_risk_metrics
            ORDER BY risk_score DESC;
            """
        )
        rows = cursor.fetchall()
        return [MerchantRiskResponse(**dict(row)) for row in rows]

@router.get("/merchant-risks/{category}", response_model=MerchantRiskResponse)
def get_merchant_risk_by_category(category: str = Path(..., description="Название категории мерчанта")):

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT merchant_category, total_alerts, total_fraud_volume, 
                   avg_fraud_amount, max_fraud_amount, risk_score, 
                   risk_level, last_updated_at
            FROM merchant_risk_metrics
            WHERE merchant_category = %s;
            """,
            (category,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Category '{category}' not found in risk metrics.")
        return MerchantRiskResponse(**dict(row))