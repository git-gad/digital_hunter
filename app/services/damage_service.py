from db.mongo_client import targets


def process_damage(data):
    entity_id = data['entity_id']

    result = data['result']

    targets.update_one(
        {'entity_id': entity_id},
        {
            '$set': {'status': result}
        }
    )