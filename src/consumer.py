# src/consumer.py

import pika
import ssl

class Consumer:
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

    def callback(self, ch, method, properties, body):
        print(f" [x] Received {body}")

    def run(self):
        self.channel.basic_consume(queue='hello',
                                   on_message_callback=self.callback,
                                   auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

        self.connection.close()
