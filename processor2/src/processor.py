import pika
import json
import logging
import time

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
NUM_PROCESSORS = 3
PREFETCH_COUNT = 10
FINISHED_QUEUE = 'finished'
PROCESSOR_NR = 2


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Processor:
    def __init__(self, processor_id):
        self.connection = None
        self.channel = None
        self.processor_id = processor_id
        self.queue_name = f"Processor_{processor_id}"

    def connect(self):
        """Create a Connection using pika.BlockingConnection."""
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_declare(queue=FINISHED_QUEUE)
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def on_message(self, ch, method, properties, body):
        """Callback for when a message is received."""
        try:
            message = json.loads(body)
            message['Processed'] = True
            logger.info(f"Processing message: {message}")
            self.channel.basic_publish(
                exchange='',
                routing_key=FINISHED_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode message: {body}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message, auto_ack=False)
        logger.info(f"Started consuming messages from {self.queue_name}")
        self.channel.start_consuming()

    def run(self):
        self.connect()
        try:
            self.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def main():
    processor = Processor(PROCESSOR_NR)
    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        processor.stop()

if __name__ == "__main__":
    time.sleep(5)
    main()

