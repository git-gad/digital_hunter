from db.mongo_client import attacks, targets

def process_attack(data):
    entity_id = data['entity_id']
    
    attack_id = data['attack_id']

    attacks.insert_one(data)

    targets.update_one(
        {'entity_id': entity_id},
        {
            '$set': {'status': 'attacked'},
            '$push': {'attacks': attack_id}
        },
        upsert=True
    )