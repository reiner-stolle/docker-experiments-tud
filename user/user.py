import os
import aio_pika
import asyncio

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    queue = await channel.declare_queue("query_queue", durable=True)

    for file_name in os.listdir("sql_queries"):
        if file_name.endswith(".sql"):
            with open(os.path.join("sql_queries", file_name), "r") as file:
                query = file.read()
                await channel.default_exchange.publish(
                    aio_pika.Message(body=query.encode()),
                    routing_key=queue.name,
                )
    await connection.close()

if __name__ == "__main__":
    asyncio.run(main())
