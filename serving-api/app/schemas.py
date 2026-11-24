from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class HealthResponse(BaseModel):
    status: str
    database_connected: bool
    timestamp: datetime

class FraudAlertResponse(BaseModel):
    id: int
    transaction_id: str
    user_id: str
    amount: float
    merchant_category: str
    alert_type: str
    event_time: datetime
    detected_at: datetime
    created_at: Optional[datetime] = None

class AlertsSummaryStats(BaseModel):
    total_alerts: int
    total_volume: float
    avg_amount: float
    max_amount: float

class MerchantRiskResponse(BaseModel):
    merchant_category: str
    total_alerts: int
    total_fraud_volume: float
    avg_fraud_amount: float
    max_fraud_amount: float
    risk_score: float
    risk_level: str
    last_updated_at: datetime