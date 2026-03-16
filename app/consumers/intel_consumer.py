from confluent_kafka import Consumer, Producer
import json
from services.intel_service import process_intel
from services.validation_service import validate_report, REQUIRED_INTEL


conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'intel-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)

consumer.subscribe(['intel'])

#  producer for dlq
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
    

while True:
    msg = consumer.poll(1.0)

    if msg and not msg.error():
        try:
            report = json.loads(msg.value().decode('utf-8'))
        
            validate_report(report, REQUIRED_INTEL)

            process_intel(report)
            
        except Exception as e:
            send_to_dlq(e, report)

        consumer.commit()
        
        