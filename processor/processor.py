import aio_pika
import asyncio
import os

async def process_query(query):
    # Simulate query processing
    return query + " -- processed"

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    processor_name = os.getenv("PROCESSOR_NAME", "processor1")
    queue = await channel.declare_queue(f"{processor_name}_queue", durable=True)
    result_queue = await channel.declare_queue("result_queue", durable=True)

    async def process_message(message: aio_pika.IncomingMessage):
        async with message.process():
            processed_query = await process_query(message.body.decode())
            await channel.default_exchange.publish(
                aio_pika.Message(body=processed_query.encode()),
                routing_key=result_queue.name,
            )

    await queue.consume(process_message)
    await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
