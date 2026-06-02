from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
def send_order_event(order_id, user_id, products):
    event = {
        'order_id': order_id,
        'user_id': user_id,
        'products': products,
        'timestamp': time.time()
    }
    producer.send('orders', value=event)
    print(f'Sent order event: {order_id}')

# Simulate sending an order
if __name__ == '__main__':
    send_order_event(
        order_id=1,
        user_id=12435,
        products=[1362, 14014, 12427]
    )
producer.flush()

