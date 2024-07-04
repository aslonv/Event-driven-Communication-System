# src/main.py

import threading
from producer import Producer
from consumer import Consumer

def main():
    producer = Producer()
    consumer = Consumer()

    producer_thread = threading.Thread(target=producer.run)
    consumer_thread = threading.Thread(target=consumer.run)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    main()
