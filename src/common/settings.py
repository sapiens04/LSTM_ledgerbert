from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    CLIENT_SECRET: str
    USER_AGENT: str
    CLIENT_ID: str

    KAFKA_HOST: str
    KAFKA_PORT: int
    KAFKA_TOPIC: str
    SUBREDDIT: str

    DB_USER: str
    DB_PASSWORD: str
    DB_PORT: int
    DB_HOST: str
    DB_DATABASE: str
    MONGO_ROOT_USERNAME: str
    MONGO_ROOT_PASSWORD: str

    RAPIDAPI_KEY: str
    RAPIDAPI_HOST: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
