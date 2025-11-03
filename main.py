import logging
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi_utils.tasks import repeat_every
import aioredis
import asyncio
from pydantic import BaseModel, Field
from typing import Optional, List

LEADERBOARD_KEY = "quiz_leaderboard"
LEADERBOARD_EXPIRE = 60 * 60 * 24  # 24 hours expiration
METADATA_KEY = "quiz_leaderboard_metadata"

app = FastAPI(title="Async Quiz Leaderboard")
router = APIRouter()

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quiz_leaderboard")

redis = None

class UserScoreUpdate(BaseModel):
    username: str = Field(..., example="alice")
    score: int = Field(..., ge=0, example=150)

class LeaderboardEntry(BaseModel):
    username: str
    score: int
    rank: int

class UserRankResponse(BaseModel):
    username: str
    score: Optional[int] = None
    rank: Optional[int] = None

class LeaderboardMetadata(BaseModel):
    total_users: int
    top_score: Optional[int]
    top_user: Optional[str]

@app.on_event("startup")
async def startup_event():
    global redis
    redis = await aioredis.from_url("redis://localhost", decode_responses=True)
    try:
        await redis.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        redis = None

@app.on_event("shutdown")
async def shutdown_event():
    global redis
    if redis:
        await redis.close()

@router.post("/score", response_model=UserRankResponse)
async def update_score(update: UserScoreUpdate):
    """Update a user's score in the leaderboard."""
    global redis
    if not redis:
        raise HTTPException(status_code=503, detail="Redis unavailable.")
    try:
        # ZADD leaderboard username score
        await redis.zadd(LEADERBOARD_KEY, {update.username: update.score})
        await redis.expire(LEADERBOARD_KEY, LEADERBOARD_EXPIRE)
        # Get reverse rank (higher scores rank higher)
        rank = await redis.zrevrank(LEADERBOARD_KEY, update.username)
        if rank is not None:
            rank = rank + 1  # 0-based index
        score = await redis.zscore(LEADERBOARD_KEY, update.username)
        return UserRankResponse(username=update.username, score=int(score or 0), rank=rank)
    except Exception as e:
        logger.error(f"Redis error in update_score: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable.")

@router.get("/leaderboard", response_model=List[LeaderboardEntry])
async def get_leaderboard(top: int = 10):
    """Get the top N users in the leaderboard."""
    global redis
    if not redis:
        raise HTTPException(status_code=503, detail="Redis unavailable.")
    try:
        # ZREVRANGE leaderboard 0 N-1 WITHSCORES
        res = await redis.zrevrange(LEADERBOARD_KEY, 0, top - 1, withscores=True)
        leaderboard = []
        for idx, (username, score) in enumerate(res):
            leaderboard.append(LeaderboardEntry(
                username=username,
                score=int(score),
                rank=idx+1
            ))
        return leaderboard
    except Exception as e:
        logger.error(f"Redis error in get_leaderboard: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable.")

@router.get("/rank/{username}", response_model=UserRankResponse)
async def get_user_rank(username: str):
    """Get the rank and score of a particular user."""
    global redis
    if not redis:
        raise HTTPException(status_code=503, detail="Redis unavailable.")
    try:
        rank = await redis.zrevrank(LEADERBOARD_KEY, username)
        score = await redis.zscore(LEADERBOARD_KEY, username)
        response = UserRankResponse(username=username)
        if rank is not None:
            response.rank = rank + 1
        if score is not None:
            response.score = int(score)
        return response
    except Exception as e:
        logger.error(f"Redis error in get_user_rank: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable.")

@router.get("/metadata", response_model=LeaderboardMetadata)
async def get_metadata():
    global redis
    if not redis:
        raise HTTPException(status_code=503, detail="Redis unavailable.")
    try:
        meta = await redis.hgetall(METADATA_KEY)
        return LeaderboardMetadata(
            total_users=int(meta.get('total_users', 0)),
            top_score=int(meta.get('top_score', 0)) if meta.get('top_score') else None,
            top_user=meta.get('top_user')
        )
    except Exception as e:
        logger.error(f"Redis error in get_metadata: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable.")

@app.on_event("startup")
@repeat_every(seconds=30, wait_first=True)
async def update_metadata_bg_task():
    """Background task to update leaderboard metadata every 30s."""
    global redis
    if not redis:
        return
    try:
        total_users = await redis.zcard(LEADERBOARD_KEY)
        top_data = await redis.zrevrange(LEADERBOARD_KEY, 0, 0, withscores=True)
        top_score, top_user = None, None
        if top_data:
            top_user, top_score = top_data[0][0], int(top_data[0][1])
        await redis.hset(METADATA_KEY, mapping={
            'total_users': total_users,
            'top_score': top_score if top_score is not None else '',
            'top_user': top_user if top_user is not None else ''
        })
        await redis.expire(METADATA_KEY, LEADERBOARD_EXPIRE)
    except Exception as e:
        logger.error(f"Error in background metadata update: {e}")

app.include_router(router, prefix="/api")
