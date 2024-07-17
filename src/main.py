# src/main.py
import threading
import logging
import signal
import sys
from producer import Producer
from consumer import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def signal_handler(signum, frame):
    logging.info("Interrupt received, stopping threads...")
    for thread in threading.enumerate():
        if thread != threading.main_thread():
            thread.join(timeout=1.0)
    sys.exit(0)

def main():
    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # RabbitMQ connection details
    rabbitmq_config = {
        'host': 'rabbitmq',
        'port': 5671,
        'queue_name': 'hello',
        'exchange_name': 'my_exchange',
        'routing_key': 'my_routing_key',
        'ca_certs': "path/to/ca_certificate.pem",
        'certfile': "path/to/client_certificate.pem",
        'keyfile': "path/to/client_key.pem"
    }

    # Create producer and consumer instances
    producer = Producer(**rabbitmq_config)
    consumer = Consumer(**rabbitmq_config)  # Assuming Consumer has similar parameters

    # Create and start threads
    producer_thread = threading.Thread(target=producer.run, name="Producer")
    consumer_thread = threading.Thread(target=consumer.run, name="Consumer")

    producer_thread.start()
    consumer_thread.start()

    logging.info("Producer and Consumer threads started")

    # Wait for threads to complete
    try:
        producer_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Stopping threads...")
    finally:
        logging.info("Exiting main thread")

if __name__ == "__main__":
    main()
