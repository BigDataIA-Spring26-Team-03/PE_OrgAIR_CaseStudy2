# app/core/deps.py
from functools import lru_cache

from app.services.redis_cache import RedisCache
from app.config import settings


@lru_cache
def get_cache() -> RedisCache:
    # settings.REDIS_URL comes from .env (local) or docker-compose env (container)
    return RedisCache(settings.REDIS_URL)


# simple global for routers to import
cache = get_cache()