from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache
from typing import Optional
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.
    """
    # Application
    APP_ENV: str = "development"
    DEBUG: bool = True
    APP_NAME: str = "PE OrgAIR Platform"
    APP_VERSION: str = "1.0.0"
    
    # Snowflake (Optional for testing, required for production)
    SNOWFLAKE_ACCOUNT: Optional[str] = None
    SNOWFLAKE_USER: Optional[str] = None
    SNOWFLAKE_PASSWORD: Optional[str] = None
    SNOWFLAKE_DATABASE: str = "PE_ORGAIR_DB"
    SNOWFLAKE_SCHEMA: str = "PE_ORGAIR_SCHEMA"
    SNOWFLAKE_WAREHOUSE: str = "PE_ORGAIR_WH"
    
    # Redis
    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    
    # AWS S3
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: Optional[str] = "us-east-1"
    S3_BUCKET: Optional[str] = None
    
    @model_validator(mode="after")
    def require_snowflake_in_production(self):
        if self.APP_ENV == "production":
            missing = [
                k for k in [
                    "SNOWFLAKE_ACCOUNT",
                    "SNOWFLAKE_USER",
                    "SNOWFLAKE_PASSWORD",
                ]
                if getattr(self, k) in (None, "")
            ]
            if missing:
                raise ValueError(
                    f"Missing Snowflake settings in production: {missing}"
                )
        return self
    
    model_config = SettingsConfigDict(
    env_file=".env",
    case_sensitive=False
)

@lru_cache
def get_settings() -> Settings:
    """
    Cached settings instance to avoid repeated env loading.
    """
    return Settings()

settings = get_settings()