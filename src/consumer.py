# src/consumer.py
import pika
import ssl
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)

class Consumer:
    def __init__(self, host: str, port: int, queue_name: str, exchange_name: str, 
                 routing_key: str, ca_certs: str, certfile: str, keyfile: str):
        # Store configuration
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
        """Establish a connection to RabbitMQ."""
        try:
            # Set up SSL context
            context = ssl.create_default_context(cafile=self.ca_certs)
            context.load_cert_chain(self.certfile, self.keyfile)
            
            ssl_options = pika.SSLOptions(context)
            
            # Set up connection parameters
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                ssl_options=ssl_options,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            # Establish connection and channel
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange, queue, and binding
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.routing_key)
            
            logging.info("Consumer connected to RabbitMQ successfully")
        except Exception as e:
            logging.error(f"Error connecting to RabbitMQ: {str(e)}")
            raise

    def callback(self, ch, method, properties, body):
        """Process received messages."""
        try:
            message = json.loads(body)
            logging.info(f"Received message: {message}")
            # Process the message here
            # For example:
            # process_message(message)
        except json.JSONDecodeError:
            logging.error(f"Received invalid JSON: {body}")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")

    def run(self):
        """Start consuming messages."""
        try:
            self.connect()
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.callback,
                auto_ack=True
            )
            logging.info(f"Consumer started. Waiting for messages on queue '{self.queue_name}'.")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Consumer stopped by user")
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
        finally:
            self.stop()

    def stop(self):
        """Stop consuming and close the connection."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logging.info("Consumer stopped and connection closed")

if __name__ == "__main__":
    # Example usage
    consumer = Consumer(
        host='rabbitmq',
        port=5671,
        queue_name='hello',
        exchange_name='my_exchange',
        routing_key='my_routing_key',
        ca_certs="path/to/ca_certificate.pem",
        certfile="path/to/client_certificate.pem",
        keyfile="path/to/client_key.pem"
    )
    consumer.run()
