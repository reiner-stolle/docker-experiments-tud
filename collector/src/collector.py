import json
import pika
import psycopg2
from psycopg2 import pool
import asyncio
import logging
import signal
from pika.adapters.asyncio_connection import AsyncioConnection
import time

# Configuration
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
POSTGRES_HOST = 'postgresql'
POSTGRES_PORT = 5432
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_DB = 'imdb'
QUEUE_NAME = 'finished'
NUM_CONSUMERS = 5  # Number of concurrent consumers

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Connection pool for PostgreSQL
postgres_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB
)

class AsyncCollector:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.consuming = False
        self.should_stop = False

    async def connect(self):
        self.connection = AsyncioConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )

    def on_connection_open(self, connection):
        logging.info("Connection opened")
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_open_error(self, connection, error):
        logging.error(f"Connection open failed: {error}")
        if not self.should_stop:
            asyncio.get_event_loop().call_later(5, self.reconnect)

    def on_connection_closed(self, connection, reason):
        logging.info(f"Connection closed: {reason}")
        self.consuming = False
        if not self.should_stop:
            asyncio.get_event_loop().call_later(5, self.reconnect)

    def reconnect(self):
        if not self.should_stop:
            asyncio.ensure_future(self.connect())

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.queue_declare(queue=QUEUE_NAME, callback=self.on_queue_declared)

    def on_channel_closed(self, channel, reason):
        logging.info(f"Channel closed: {reason}")
        if not self.should_stop:
            self.connection.close()

    def on_queue_declared(self, frame):
        self.channel.basic_qos(prefetch_count=1)
        self.start_consuming()

    def start_consuming(self):
        self.consuming = True
        self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=self.on_message)
        logging.info('[COLLECTOR] Waiting for messages. To exit press CTRL+C')

    def on_message(self, channel, basic_deliver, properties, body):
        message = json.loads(body.decode('utf-8'))

        if message.get('Processed') and message.get('Dispatched'):
            query = message.get('Query')
            if query:
                logging.info(f"Executing Query with {message['QID']}")
                asyncio.create_task(self.execute_query(query))
            else:
                logging.warning("No query found in the message.")
            channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)
        else:
            channel.basic_nack(delivery_tag=basic_deliver.delivery_tag, requeue=True)

    async def execute_query(self, query):
        conn = None
        cur = None
        try:
            conn = postgres_pool.getconn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            results = cur.fetchall() if cur.description else None
            if results:
                for row in results:
                    logging.info(row)
            else:
                logging.info("Query executed successfully, no results to fetch.")
        except psycopg2.Error as e:
            logging.error(f"Error executing query in PostgreSQL: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                postgres_pool.putconn(conn)

    async def run(self):
        await self.connect()
        while not self.should_stop:
            await asyncio.sleep(0.1)  # Allow other tasks to run

    async def stop(self):
        self.should_stop = True
        if self.consuming:
            self.channel.stop_consuming()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

async def main():
    collectors = [AsyncCollector() for _ in range(NUM_CONSUMERS)]
    tasks = [asyncio.create_task(collector.run()) for collector in collectors]
    
    def signal_handler():
        for task in tasks:
            task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.info("Shutting down...")
    finally:
        await asyncio.gather(*(collector.stop() for collector in collectors))
        postgres_pool.closeall()

if __name__ == "__main__":
    time.sleep(5)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down...")
    finally:
        # Ensure that any remaining connections in the pool are closed
        if 'postgres_pool' in globals():
            postgres_pool.closeall()
        logging.info("Shutdown complete.")
