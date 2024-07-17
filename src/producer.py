# src/producer.py
import pika
import ssl
import json
import time
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)

class Producer:
    def __init__(self, host: str, port: int, queue_name: str, exchange_name: str, 
                 routing_key: str, ca_certs: str, certfile: str, keyfile: str):
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            context = ssl.create_default_context(cafile=self.ca_certs)
            context.load_cert_chain(self.certfile, self.keyfile)
            
            ssl_options = pika.SSLOptions(context)
            
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                ssl_options=ssl_options,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.routing_key)
            
            logging.info("Connected to RabbitMQ successfully")
        except Exception as e:
            logging.error(f"Error connecting to RabbitMQ: {str(e)}")
            raise

    def publish_message(self, message: Dict[str, Any]):
        if not self.connection or self.connection.is_closed:
            self.connect()
        
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            logging.info(f"Published message: {message}")
        except pika.exceptions.AMQPError as e:
            logging.error(f"Error publishing message: {str(e)}")
            self.connection.close()
            self.connect()  # Try to reconnect

    def run(self, interval: int = 5):
        try:
            self.connect()
            message_count = 0
            while True:
                message = {
                    "id": message_count,
                    "content": f"Hello World {message_count}!",
                    "timestamp": time.time()
                }
                self.publish_message(message)
                message_count += 1
                time.sleep(interval)
        except KeyboardInterrupt:
            logging.info("Producer stopped by user")
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logging.info("Connection closed")

if __name__ == "__main__":
    producer = Producer(
        host='rabbitmq',
        port=5671,
        queue_name='hello',
        exchange_name='my_exchange',
        routing_key='my_routing_key',
        ca_certs="path/to/ca_certificate.pem",
        certfile="path/to/client_certificate.pem",
        keyfile="path/to/client_key.pem"
    )
    producer.run()
