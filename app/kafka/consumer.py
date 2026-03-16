from kafka.kafka_client import consumer, send_to_dlq
from utils.logger import log_event
import json
from services.intel_service import process_intel
from services.attack_service import process_attack
from services.damage_service import process_damage
from services.validation_service import validate_report, REQUIRED_INTEL, REQUIRED_ATTACK, REQUIRED_DAMAGE


def run():
    while True:
        report = None
        raw_value = None
        handled = False
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        
        if msg.error():
            continue
        
        topic = msg.topic()

        try:
            raw_value = msg.value()
            if raw_value is None:
                log_event('error', 'no message value')
            
            report = json.loads(raw_value.decode('utf-8'))
        
            if topic == 'intel':
                validate_report(report, REQUIRED_INTEL)
                
                process_intel(report)
                log_event('info', 'intel message processed')
            
            elif topic == 'attack':
                validate_report(report, REQUIRED_ATTACK)
                
                process_attack(report)
                log_event('info', 'attack message processed')
                
            elif topic == 'damage':
                validate_report(report, REQUIRED_DAMAGE)
            
                process_damage(report)
                log_event('info', 'damage message processed')
            
            handled = True
                
        except Exception as e:
            if isinstance(raw_value, (bytes, bytearray)):
                raw_value = raw_value.decode('utf-8', errors='replace')
            send_to_dlq(
                e,
                report if report is not None else {'raw_value': raw_value}
            )
            handled = True
            
        finally:
            if handled:
                consumer.commit()
