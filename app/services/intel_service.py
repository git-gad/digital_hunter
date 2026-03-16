from db.mongo_client import targets
from utils.haversine import haversine_km

def process_intel(data):
    entity_id = data['entity_id']
    lat = data['reported_lat']
    lon = data['reported_lon']
    timestamp = data['timestamp']
    
    target = targets.find_one({'entity_id': entity_id})
    
    if not target:
        targets.insert_one({
            'timestamp': timestamp,
            'signal_id': data['signal_id'],
            'entity_id': entity_id,
            'lat': lat,
            'lon': lon,
            'priority_level': 99,
            'status': 'active',
            'distance': 0
        })
        
        return
        
    distance = haversine_km(target['lat'], target['lon'], lat, lon)
    
    targets.update_one(
        {'entity_id': entity_id},
        {
            '$set': {
                'lat': lat,
                'lon': lon,
                'timestamp': timestamp,
                'distance': distance
            }
        }
    )