from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Fraud Detection Serving API"
    app_version: str = "1.0.0"

    postgres_host: str = "localhost"
    postgres_port: int = 5435
    postgres_db: str = "fraud_detection"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres_password"

settings = Settings()