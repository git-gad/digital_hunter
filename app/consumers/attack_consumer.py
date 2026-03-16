from confluent_kafka import Consumer
import json
from intel_consumer import send_to_dlq
from services.attack_service import process_attack
from services.validation_service import validate_report, REQUIRED_ATTACK


conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'attack-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)

consumer.subscribe(['attack'])


while True:
    msg = consumer.poll(1.0)

    if msg and not msg.error():
        try:
            report = json.loads(msg.value().decode('utf-8'))
            print(f'attack consumed {report}', flush=True)
            
            validate_report(report, REQUIRED_ATTACK)

            process_attack(report)
            print('attack processed', flush=True)
            
        except Exception as e:
            send_to_dlq(e, report)

        consumer.commit()