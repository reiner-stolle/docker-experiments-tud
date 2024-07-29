import os
import aio_pika
import asyncio

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    queue = await channel.declare_queue("query_queue", durable=True)

    # Path to the SQL files in the Docker container
    sql_folder_path = "./job_queries"

    for file_name in os.listdir(sql_folder_path):
        if file_name.endswith(".sql"):
            file_path = os.path.join(sql_folder_path, file_name)
            with open(file_path, "r") as file:
                query = file.read()
                await channel.default_exchange.publish(
                    aio_pika.Message(body=query.encode()),
                    routing_key=queue.name,
                )
    await connection.close()

if __name__ == "__main__":
    asyncio.run(main())
