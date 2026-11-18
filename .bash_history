sudo apt update && sudo apt upgrade -y
nano .env
pip install confluent-kafka
[200~pip install confluent-kafka
apt install cofluent-kafka
sudo apt update
sudo apt install python3-pip -y
pip install confluent-kafka
python3 -m venv venv
apt install python3.12-venv
python3 -m venv venv
source venv/bin/activate
pip install confluent-kafka
nano .env
nano .env
cat << 'EOF' > producer_network_log.py
import os
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET')
}

producer = Producer(conf)
print("Kafka Producer Initialized Successfully!")
EOF

python3 producer_network_log.py
source venv/bin/activate
python3 producer_network_log.py
export KAFKA_BOOTSTRAP_SERVERS=lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092
export KAFKA_API_KEY=BJD3X5HK67B7RAJL
export KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
export KAFKA_BOOTSTRAP_SERVERS=lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092
export KAFKA_API_KEY=BJD3X5HK67B7RAJL
export KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
python3 producer_network_log.py
export KAFKA_BOOTSTRAP_SERVERS="lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092"
export KAFKA_API_KEY="BJD3X5HK67B7RAJL"
export KAFKA_API_SECRET="cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA"
python3 producer_network_log.py
echo $KAFKA_BOOTSTRAP_SERVERS
export KAFKA_BOOTSTRAP_SERVERS="lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092"
export KAFKA_API_KEY="BJD3X5HK67B7RAJL"
export KAFKA_API_SECRET="cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA"
python3 producer_network_log.py
cat << 'EOF' > producer_network_log.py
import os
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'BJD3X5HK67B7RAJL',
    'sasl.password': 'cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA'
}

producer = Producer(conf)
print("Kafka Producer Initialized Successfully!")
EOF

python3 producer_network_log.py
ping -c 3 lkc-xqwvo3q.us-east1.gcp.confluent.cloud
echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf
ping -c 3 lkc-xqwvo3q.us-east1.gcp.confluent.cloud
cat /etc/resolv.conf
cat << 'EOF' > producer_network_log.py
import os
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'BJD3X5HK67B7RAJL',
    'sasl.password': 'cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA'
}

producer = Producer(conf)
print("Kafka Producer Initialized Successfully!")
EOF

python3 producer_network_log,py
python3 producer_network_log.py
pip install python-dotenv
cat << 'EOF' > .env
KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092
KAFKA_API_KEY=BJD3X5HK67B7RAJL
KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
EOF

cat << 'EOF' > producer.py
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
    'security.protocol': 'SASL_PLAINTEXT',
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
EOF

python3 producer.py
cat << 'EOF' > .env
KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092
KAFKA_API_KEY=BJD3X5HK67B7RAJL
KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
EOF

python3 producer.py
[200~rm -f .env && cat << 'EOF' > .env
KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092
KAFKA_API_KEY=BJD3X5HK67B7RAJL
KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
EOF~
[200~rm -f .env && cat << 'EOF' > .env
KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092
KAFKA_API_KEY=BJD3X5HK67B7RAJL
KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
EOF~
python3 producer.py
rm -f .env
echo "KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092" > .env

echo "KAFKA_API_KEY=BJD3X5HK67B7RAJL" >> .env
echo "KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA" >> .env
python3 producer.py
cat .env
clear

clear
cat .env
python3 producer.py
source venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS=lkc-xqwvo3q.us-east1.gcp.confluent.cloud:9092
export KAFKA_API_KEY=BJD3X5HK67B7RAJL
export KAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA
python3 producer_network_log.py
ls
nano producer_network_log.py
rm -f .env
printf "KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092\nKAFKA_API_KEY=BJD3X5HK67B7RAJ\nKAFKA_API_SECRET=cfltveWT/C5C+wCKgQ08hHDhOGAGnh980ljoL/h86p3GWLk6ngf9nHk2R13iU+IA" > .env
cat .env
cat << 'EOF' > .gitignore
venv/
.env
*.db
__pycache__/
*.pyc
EOF

cat << 'EOF' > producer.py
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
    'security.protocol': 'SASL_PLAINTEXT',
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
EOF

ls -la
python3 prooducer.py
python3  producer.py
source venv/bin/activate
pip install python-dotenv confluent-kafka
python3 producer.py
sed -i "s/'security.protocol': 'SASL_PLAINTEXT'/'security.protocol': 'SASL_SSL'/g" producer.py
python3 producer.py
rm  -f  .env
printf "KAFKA_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092\nKAFKA_API_KEY=DKXHLGNXY43I7AI4\nKAFKA_API_SECRET=cfltAgNh9k2YxHr+Us4flXq6Z8HiTvkSXza9mQmsMcfOVx9NmZ9/qjaiDsDUrfzg\n" > .env
cat .env
python3 producer.py
