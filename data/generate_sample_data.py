"""
Sample Data Generator
Generates synthetic movie viewing events for batch processing
"""

import json
import random
from datetime import datetime, timedelta
import os

def generate_batch_events(num_events=10000, output_file="sample_events.json"):
    """Generate sample batch events"""
    events = []
    base_time = datetime.now() - timedelta(days=7)
    
    for i in range(num_events):
        event = {
            'event_id': f"evt_{int(base_time.timestamp() * 1000)}_{i}",
            'user_id': random.randint(1, 1000),
            'movie_id': random.randint(1, 500),
            'event_type': random.choice(['view', 'pause', 'resume', 'complete', 'rating']),
            'timestamp': (base_time + timedelta(seconds=random.randint(0, 604800))).isoformat(),
            'duration': random.randint(30, 7200) if random.random() > 0.3 else None,
            'rating': random.randint(1, 5) if random.random() > 0.7 else None
        }
        events.append(event)
    
    output_path = os.path.join(os.path.dirname(__file__), output_file)
    with open(output_path, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"Generated {num_events} events to {output_path}")
    return output_path

if __name__ == "__main__":
    generate_batch_events(10000)
