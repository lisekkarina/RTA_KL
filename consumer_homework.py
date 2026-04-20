from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detectors'
)

user_transactions = defaultdict(list)
print("Nasluchuje anomalii (wiecej niz 3 transakcje tego samego usera w 60s)...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    current_time = time.time()
    
    user_transactions[user_id].append(current_time)
    user_transactions[user_id] = [t for t in user_transactions[user_id] if current_time - t <= 60]
    
    if len(user_transactions[user_id]) > 3:
        print(f"⚠️ ALERT ANOMALII! Uzytkownik {user_id} wykonal {len(user_transactions[user_id])} transakcji w 60s!")
        user_transactions[user_id] = []