from confluent_kafka import Consumer
import json


conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'damage-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)

consumer.subscribe(['damage'])


while True:
    msg = consumer.poll(1.0)

    if msg and not msg.error():
        doc = json.loads(msg.value().decode('utf-8'))

        print(doc, flush=True)