import redis
import os
import dotenv
import json
ENVLOC = '/app/.env'
dotenv.load_dotenv(ENVLOC)
# Centralized Redis connection
# Use a single connection pool that is shared across the application.
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

r_alg = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=1,
    decode_responses=True
)
TEST = "stocks"
FILEPATH = os.getenv("FILEPATH")
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS = list(json.load(open(os.path.join(CONFIG_DIR, "stocks.json")))[TEST].keys())

DECTECTION_TYPE = 'buy'

VOLUME = True

if __name__ == "__main__":
    
    print(STOCKS)