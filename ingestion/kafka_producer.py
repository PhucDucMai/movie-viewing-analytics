"""
Kafka Producer for Movie Viewing Events
Simulates movie viewing behavior data ingestion
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import yaml
import os

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def generate_movie_event(user_id=None, movie_id=None):
    """Generate a synthetic movie viewing event"""
    if user_id is None:
        user_id = random.randint(1, 10000)
    if movie_id is None:
        movie_id = random.randint(1, 5000)
    
    event = {
        'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        'user_id': user_id,
        'movie_id': movie_id,
        'event_type': random.choice(['view', 'pause', 'resume', 'complete', 'rating']),
        'timestamp': datetime.now().isoformat(),
        'duration': random.randint(30, 7200) if random.random() > 0.3 else None,
        'rating': random.randint(1, 5) if random.random() > 0.7 else None
    }
    return event

def main():
    """Main producer loop"""
    config = load_config()
    kafka_config = config['kafka']
    
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    topic = kafka_config['topic']
    print(f"Starting Kafka producer for topic: {topic}")
    
    try:
        event_count = 0
        while True:
            event = generate_movie_event()
            producer.send(topic, value=event, key=str(event['user_id']))
            event_count += 1
            
            if event_count % 100 == 0:
                print(f"Sent {event_count} events to Kafka")
            
            time.sleep(random.uniform(0.1, 2.0))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
