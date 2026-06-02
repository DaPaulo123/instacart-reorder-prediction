from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['instacart']

db['user_features'].create_index('user_id', unique=True)
db['product_features'].create_index('product_id', unique=True)
db['up_features'].create_index([('user_id', 1), ('product_id', 1)], unique=True)

print('user_features indexes:', list(db['user_features'].index_information().keys()))
print('product_features indexes:', list(db['product_features'].index_information().keys()))
print('up_features indexes:', list(db['up_features'].index_information().keys()))

print('hello')
db['user_features'].count_documents({})
print('hi')
# Save user list
