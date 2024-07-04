# src/producer.py

import pika
import ssl
import json

class Producer:
    def __init__(self):
        context = ssl.create_default_context(
            cafile="path/to/ca_certificate.pem"
        )
        context.load_cert_chain("path/to/client_certificate.pem", "path/to/client_key.pem")

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                port=5671,
                ssl_options=pika.SSLOptions(context)
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hello')

    def run(self):
        while True:
            message = {"content": "Hello World!"}
            self.channel.basic_publish(exchange='',
                                       routing_key='hello',
                                       body=json.dumps(message))
            print(" [x] Sent 'Hello World!'")

        self.connection.close()
