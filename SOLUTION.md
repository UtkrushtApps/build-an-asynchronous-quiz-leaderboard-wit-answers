# Solution Steps

1. Install the required dependencies: fastapi, uvicorn, aioredis, fastapi-utils, and pydantic.

2. Create the main FastAPI application in 'main.py' and import the required packages, including logging, FastAPI, APIRouter, aioredis, and typing.

3. Define Pydantic models: UserScoreUpdate (for updating user scores), LeaderboardEntry (for leaderboard output), UserRankResponse (for user rank queries), and LeaderboardMetadata (for metadata).

4. Set leaderboard and metadata Redis keys, and define expiration duration for the keys for automatic cleanup.

5. On application startup, create the Redis connection pool asynchronously, and handle connection errors gracefully (logging and refusing requests if unavailable).

6. Handle shutdown by properly closing the Redis connection.

7. Implement the '/api/score' POST endpoint: asynchronously upsert a user's score using ZADD. Set the expiration for the leaderboard key after each write. Return the user's new score and rank using ZREVRANK and ZSCORE.

8. Implement the '/api/leaderboard' GET endpoint: asynchronously retrieve the top N (default 10) users from the leaderboard using ZREVRANGE WITHSCORES, and format the result with rank.

9. Implement the '/api/rank/{username}' GET endpoint: asynchronously retrieve the user's score and current rank using ZREVRANK and ZSCORE.

10. Implement the '/api/metadata' GET endpoint: asynchronously retrieve leaderboard metadata (current user count, top score/user) by reading from a Redis hash.

11. Use FastAPI Utils's repeat_every to set up a background task that periodically (every 30 seconds) updates the leaderboard metadata key with total user count, top user, and top score using ZCARD and ZREVRANGE.

12. Log Redis-related errors, and use HTTP 503 if Redis is unavailable for requests.

13. Bundle the API endpoints into a router with a defined prefix. Attach the router to the app.

14. Ensure your 'requirements.txt' includes all dependencies for deployment and development: fastapi, uvicorn, aioredis, pydantic, fastapi-utils.

