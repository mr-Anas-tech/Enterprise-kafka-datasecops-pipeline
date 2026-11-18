import os
import time
import json
import random
import uuid
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException

load_dotenv()

conf = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get("KAFKA_API_KEY"),
    'sasl.password': os.environ.get("KAFKA_API_SECRET"),
    'enable.idempotence': True,               
    'acks': 'all',                            
    'queue.buffering.max.messages': 1000000,  
    'linger.ms': 20,                          
    'batch.num.messages': 10000,              
    'compression.type': 'lz4',                
    'socket.keepalive.enable': True,
    'retries': 5
}

TOPIC_NAME = "secops_networks_logs"
LOCAL_DB = "fallback_secops_buffer.db"

def init_local_buffer():
    conn = sqlite3.connect(LOCAL_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS failed_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_to_local_buffer(payload: str):
    try:
        conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO failed_logs (payload) VALUES (?)", (payload,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ [CRITICAL] Failed to write to local fallback buffer: {e}")

def flush_local_buffer(producer):
    try:
        conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT id, payload FROM failed_logs LIMIT 500")
        rows = cursor.fetchall()
        
        if rows:
            print(f"🔄 [RECOVERY] Flushing {len(rows)} cached logs from local disk to Kafka...")
            for row_id, payload in rows:
                producer.produce(TOPIC_NAME, payload.encode('utf-8'))
                cursor.execute("DELETE FROM failed_logs WHERE id = ?", (row_id,))
            conn.commit()
            producer.poll(0)
            print("✅ [RECOVERY] Local buffer batch flushed successfully!")
            
        conn.close()
    except Exception as e:
        print(f"⚠️ [RECOVERY WARNING] Error flushing local buffer: {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f"⚠️ Delivery failed: {err}")
        if msg and msg.value():
            save_to_local_buffer(msg.value().decode('utf-8'))

def create_resilient_producer():
    attempt = 0
    while True:
        try:
            p = Producer(conf)
            print("🚀 [SUCCESS] Connected securely to Confluent Cloud Kafka!")
            return p
        except Exception as e:
            attempt += 1
            sleep_time = min(60, (2 ** attempt)) + random.uniform(0, 1)
            print(f"🔌 [CIRCUIT BREAKER] Connection attempt {attempt} failed: {e}")
            print(f"⏳ Retrying connection in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

country_mapping = {
    "PK": "Pakistan", "US": "United States", "RU": "Russia", 
    "CN": "China", "DE": "Germany", "GB": "United Kingdom"
}
endpoints = ["/api/v1/login", "/api/v1/products", "/admin", "/api/v1/checkout", "/api/v1/token"]
methods = ["GET", "POST", "DELETE", "PUT"]

def generate_secops_event() -> str:
    is_attack = random.random() < 0.18
    status = 401 if is_attack else random.choice([200, 200, 200, 304, 201])
    code = "RU" if is_attack else random.choice(list(country_mapping.keys()))
    
    log_data = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": f"185.220.101.{random.randint(1, 255)}" if is_attack else f"192.168.1.{random.randint(10, 200)}",
        "country_code": code,
        "country_name": country_mapping[code],
        "endpoint": "/api/v1/login" if is_attack else random.choice(endpoints),
        "http_method": "POST" if is_attack else random.choice(methods),
        "status_code": status,
        "user_id": None if is_attack else f"usr_{random.randint(5000, 9999)}",
        "bytes_transferred": random.randint(15000, 45000) if is_attack else random.randint(150, 5200),
        "response_time_ms": random.randint(600, 1200) if is_attack else random.randint(25, 180)
    }
    return json.dumps(log_data)

def main():
    init_local_buffer()
    producer = create_resilient_producer()
    
    print("🛡️ [ENTERPRISE SEC-OPS ENGINE] Live streaming initiated...")
    
    try:
        while True:
            flush_local_buffer(producer)
            payload = generate_secops_event()
            
            try:
                producer.produce(
                    TOPIC_NAME,
                    payload.encode('utf-8'),
                    callback=delivery_report
                )
            except BufferError:
                print("⚠️ [BUFFER FULL] Local queue saturated. Caching locally...")
                save_to_local_buffer(payload)
                producer.poll(1)
            
            producer.poll(0)
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n🛑 [SHUTDOWN] Gracefully terminating pipeline...")
        producer.flush(timeout=5.0)
        print("✅ [SHUTDOWN COMPLETE] All queues flushed. Goodbye Anas!")

if __name__ == "__main__":
    main()
