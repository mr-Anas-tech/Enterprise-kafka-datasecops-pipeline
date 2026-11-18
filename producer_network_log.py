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
