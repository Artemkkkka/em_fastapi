from datetime import datetime, time, timedelta
import functools
import json
import inspect

from redis.asyncio import Redis


redis = Redis.from_url(
    "redis://localhost:6379",
    encoding="utf-8",
    decode_responses=True,
)

def compute_ttl() -> int:
    now = datetime.now()
    today_reset = datetime.combine(now.date(), time(hour=14, minute=11))
    if now >= today_reset:
        reset = today_reset + timedelta(days=1)
    else:
        reset = today_reset
    return int((reset - now).total_seconds())


def cache_response(prefix: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            key_body = json.dumps({"args": args[1:], "kwargs": kwargs}, sort_keys=True, default=str)
            key = f"{prefix}:{key_body}"
            cached = await redis.get(key)
            if cached:
                return json.loads(cached)
            result = await func(*args, **kwargs)
            if hasattr(result, "model_dump_json"):
                payload = result.model_dump()
            elif hasattr(result, "dict"):
                payload = result.dict()
            else:
                payload = result
            await redis.set(key, json.dumps(payload, default=str), ex=compute_ttl())
            return result
        wrapper.__signature__ = inspect.signature(func)
        return wrapper
    return decorator
