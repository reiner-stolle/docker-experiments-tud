import aio_pika
import asyncio
import logging
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'dbname': 'imdb',
    'user': 'user',
    'password': 'password',
    'host': 'postgresql',  # Change this if the host is different
    'port': 5432
}

async def execute_query(query):
    logger.info(f"Executing query: {query}")

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error executing query: {e}")

    await asyncio.sleep(1)  # Simulate query execution

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    result_queue = await channel.declare_queue("result_queue", durable=True)

    async def process_message(message: aio_pika.IncomingMessage):
        async with message.process():
            query = message.body.decode()
            await execute_query(query)

    await result_queue.consume(process_message)
    logger.info("Collector is running and consuming messages.")
    await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
