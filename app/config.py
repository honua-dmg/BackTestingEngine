import redis
import os
import dotenv
import json
import boto3
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
s3 = boto3.client(
    's3',
    region_name=os.getenv('DIGITALOCEAN_REGION'),
    endpoint_url=os.getenv('DIGITALOCEAN_ENDPOINT'),
    aws_access_key_id=os.getenv('DIGITALOCEAN_KEY_ID'),
    aws_secret_access_key=os.getenv('DIGITALOCEAN_KEY_SECRET')
)

TEST = "stocks"
FILEPATH = os.getenv("FILEPATH")
SIMULATION_DATE = "2025-07-11"

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS = list(json.load(open(os.path.join(CONFIG_DIR, "stocks.json")))[TEST].keys())

EXCHANGE = json.load(open(os.path.join(CONFIG_DIR, "stocks.json")))[TEST][STOCKS[0]][0]
DECTECTION_TYPE = 'buy'
GRAPH = f"{EXCHANGE}:{STOCKS[0]}"
VOLUME = True


    