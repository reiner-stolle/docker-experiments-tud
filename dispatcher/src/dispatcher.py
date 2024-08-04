import pika
import json
import logging
from pika.adapters.select_connection import SelectConnection
import time

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
ENTRY_QUEUE = 'hello'
NUM_PROCESSORS = 3
PREFETCH_COUNT = 10
BATCH_SIZE = 2

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessorAssigner:
    def __init__(self):
        self.count = 0

    def get_next(self):
        """Get the next processor to use."""
        self.count = (self.count % NUM_PROCESSORS) + 1
        return self.count

class EfficientDispatcher:
    def __init__(self):
        self.connection = None
        self.channels = {}
        self.processor_assigner = ProcessorAssigner()
        self.batch = []
        self.consuming = False

    def connect(self):
        """Create a Connection using SelectConnection."""
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        self.connection = SelectConnection(
            parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )

    def on_connection_open(self, connection):
        """Open channels."""
        logger.info("Connection opened")
        self.open_channel('entry')

    def open_channel(self, channel_type, processor_id=None):
        """Open a channel."""
        if self.connection.is_open:
            if channel_type == 'entry':
                logger.info("Opening entry channel")
                self.connection.channel(on_open_callback=self.on_entry_channel_open)
            elif channel_type == 'processor' and processor_id:
                channel_name = f'processor_{processor_id}'
                if channel_name in self.channels:
                    logger.info(f"Reusing channel for processor {processor_id}")
                    self.on_processor_channel_open(self.channels[channel_name], processor_id)
                else:
                    logger.info(f"Opening channel for processor {processor_id}")
                    self.connection.channel(on_open_callback=lambda channel: self.on_processor_channel_open(channel, processor_id))
        else:
            logger.warning(f"Cannot open channel: Connection is not open")

    def on_connection_open_error(self, connection, error):
        logger.error(f"Connection open failed: {error}")
        self.reconnect()

    def on_connection_closed(self, connection, reason):
        logger.warning(f"Connection closed: {reason}")
        self.consuming = False
        self.reconnect()

    def reconnect(self):
        """Reconnect to RabbitMQ."""
        logger.info("Reconnecting to RabbitMQ...")
        if self.connection and not self.connection.is_closed:
            self.connection.ioloop.stop()
        self.connect()

    def on_entry_channel_open(self, channel):
        logger.info("Entry channel opened")
        self.channels['entry'] = channel
        channel.add_on_close_callback(self.on_channel_closed)
        channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        channel.queue_declare(queue=ENTRY_QUEUE, callback=self.on_entry_queue_declared)

    def on_processor_channel_open(self, channel, processor_id):
        logger.info(f"Channel opened for processor {processor_id}")
        channel_name = f'processor_{processor_id}'
        self.channels[channel_name] = channel
        channel.add_on_close_callback(self.on_channel_closed)
        channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        queue_name = f"Processor_{processor_id}"
        channel.queue_declare(queue=queue_name)

    def on_channel_closed(self, channel, reason):
        logger.warning(f"Channel closed: {reason}")
        for processor_id, ch in list(self.channels.items()):
            if ch == channel:
                del self.channels[processor_id]
                if processor_id == 'entry':
                    self.open_channel('entry')
                else:
                    processor_id = int(processor_id.split('_')[1])
                    self.open_channel('processor', processor_id)
                break

    def on_entry_queue_declared(self, frame):
        self.channels['entry'].basic_consume(queue=ENTRY_QUEUE, on_message_callback=self.on_message, auto_ack=False)
        for i in range(1, NUM_PROCESSORS + 1):
            self.open_channel('processor', i)
        if not self.consuming:
            self.start_consuming()

    def start_consuming(self):
        """Start consuming messages."""
        self.consuming = True
        logger.info("Started consuming messages")

    def on_message(self, channel, method, properties, body):
        """Callback for when a message is received."""
        try:
            message = json.loads(body)
            if not message.get('Dispatched', False):
                processor_id = self.processor_assigner.get_next()
                message['Processor'] = processor_id
                message['Dispatched'] = True
                self.batch.append((method.delivery_tag, message, processor_id))
                if len(self.batch) >= BATCH_SIZE:
                    self.publish_batch()
            else:
                logger.info(f"[IGNORING] Message already dispatched, ignoring: {message.get('QID', 'Unknown')}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode message: {body}")
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def publish_batch(self):
        """Publish a batch of messages."""
        if not self.batch:
            return

        logger.info(f"Publishing batch of {len(self.batch)} messages")
        for delivery_tag, message, processor_id in self.batch:
            queue_name = f"Processor_{processor_id}"
            channel_name = f'processor_{processor_id}'
            if channel_name in self.channels:
                self.channels[channel_name].basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
                )
                self.channels['entry'].basic_ack(delivery_tag=delivery_tag)
        self.batch.clear()

    def run(self):
        self.connect()
        try:
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.consuming = False
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        self.connection.ioloop.stop()

def main():
    dispatcher = EfficientDispatcher()
    try:
        dispatcher.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        dispatcher.stop()

if __name__ == "__main__":
    time.sleep(5)
    main()

