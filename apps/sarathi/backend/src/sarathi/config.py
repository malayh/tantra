from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SECRET_KEY: str
    OPENAI_BASE_URL: str
    OPENAI_API_KEY: str
    SARATHI_MODELS: str
    DATABASE_URL: str = "postgresql+psycopg://sarathi:sarathi@localhost:5432/sarathi"
    BRAVE_API_KEY: str = ""
    EMBEDDING_MODEL: str | None = None
    SARATHI_CONTEXT_WINDOW: int | None = None
    UPLOAD_DIR: str = "/data/uploads"
    CORS_ORIGINS: str = "http://localhost:3000"

    @property
    def models(self) -> list[str]:
        return [m.strip() for m in self.SARATHI_MODELS.split(",") if m.strip()]

    @property
    def default_model(self) -> str:
        models = self.models
        if not models:
            raise ValueError("SARATHI_MODELS must list at least one model")
        return models[0]

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


@cache
def get_settings() -> Settings:
    return Settings()
