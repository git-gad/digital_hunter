REQUIRED_INTEL = ['timestamp',
                  'signal_id',
                  'entity_id',
                  'reported_lat',
                  'reported_lon']

REQUIRED_ATTACK = ['timestamp',
                   'attack_id',
                   'entity_id']

REQUIRED_DAMAGE = []


def validate_report(mess, required):
    for field in required:
        if field not in mess:
            raise ValueError(f'missing required field {field}')