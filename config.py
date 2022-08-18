"""Default config for pydantic"""
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    """Default Settings class

    It takes default configuration from the `.env` file
    """
    is_base_api_url: str = Field(..., env='IDEASCALE_BASE_API_URL')
    is_api_token: str = Field(..., env='IDEASCALE_API_TOKEN')

    class Config:
        """Sub class to define the `.env` file"""
        env_file = '.env'
        env_file_encoding = 'utf-8'

settings = Settings()
