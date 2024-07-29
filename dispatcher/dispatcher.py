import aio_pika
import asyncio

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    query_queue = await channel.declare_queue("query_queue", durable=True)
    
    processors = ["processor1_queue", "processor2_queue"]
    processor_count = len(processors)
    current_processor = 0

    async def process_message(message: aio_pika.IncomingMessage):
        nonlocal current_processor
        async with message.process():
            target_queue = processors[current_processor]
            await channel.default_exchange.publish(
                aio_pika.Message(body=message.body),
                routing_key=target_queue,
            )
            current_processor = (current_processor + 1) % processor_count

    await query_queue.consume(process_message)
    await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())