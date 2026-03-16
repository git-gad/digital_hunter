from confluent_kafka import Producer, Consumer
import json


conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'intel-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)

consumer.subscribe(['intel', 'attack', 'damage'])

producer = Producer({
    'bootstrap.servers': 'kafka:9092'
})


def delivery_report(err, msg):
    if err:
        print(f'delivery failed: {err}')
    else:
        print(f'delivered to: {msg.topic()}')
        
        
def send_to_dlq(error, report):
    payload = {
        'error': repr(error),
        'message': report
    }
    
    producer.produce(
        'intel_signals_dlq',
        value=json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    
    producer.flush(5)
