from fastapi import FastAPI
from pymongo import MongoClient
import redis
import json

app = FastAPI()

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

@app.get('/')
def root():
    return {'message': 'Instacart Reorder Prediction API'}

@app.get('/predict/{user_id}')
def get_prediction(user_id: int):
    key = f'prediction:{user_id}'
    cached = redis_client.get(key)
    if cached:
        return {'user_id': user_id, 'predictions': json.loads(cached), 'source': 'redis'}
    return {'user_id': user_id, 'predictions': None, 'message': 'No prediction found'}

@app.get('/health')
def health():
    return {'status': 'ok'}