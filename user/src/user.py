import pika
import json
import os
import re
import time

ABSOLUTE_DIR = '/app/job_queries'
BATCH_SIZE = 500                # Number of messages to send per batch
DELAY_BETWEEN_BATCHES = 0       # Delay in seconds between each batch
NUM_CHANNELS = 3                # Number of channels to create

# Configuration for RabbitMQ
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
QUEUE_NAME = 'hello'  # Use a different queue name

def establish_connection():
    print("establishing connection")
    try:
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        connection = pika.BlockingConnection(parameters)
        channels = [connection.channel() for _ in range(NUM_CHANNELS)]
        # Declare queue, which will create it if it doesn't exist
        for channel in channels:
            channel.queue_declare(queue=QUEUE_NAME)
            print("created queue with Queue name " + QUEUE_NAME)
        return connection, channels
    except Exception as e:
        print(f"Failed to establish RabbitMQ connection: {e}")
        raise

def close_connection(connection):
    try:
        if connection and connection.is_open:
            connection.close()
    except Exception as e:
        print(f"Failed to close RabbitMQ connection: {e}")
        raise

def send_message_batch(channel, messages, queue_name, channel_index):
    print("In send_message")
    try:
        for message in messages:
            # Publish each message to the queue
            channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))
            print(f"[Channel {channel_index}] Sent message: {message['QID']}")
    except Exception as e:
        print(f"Failed to send messages: {e}")
        raise

def parse_sql_files(directory):
    query_list = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as file:
                sql_content = file.read()
                
                # Split SQL content into individual queries
                queries = re.split(r';\s*\n*', sql_content)
                
                # Filter out empty queries (usually caused by trailing semicolons)
                queries = [query.strip() for query in queries if query.strip()]

                # Create message dictionaries for each query
                for idx, query_text in enumerate(queries):
                    message = {
                        'QID': f"{filename}_{idx+1}",  # Unique ID using filename and index
                        'Query': query_text,
                        'Dispatched': False,
                        'Processed': False,  
                        'Processor': 1
                    }
                    query_list.append(message)

    return query_list

def process_batches(channels, messages, queue_name):
    num_batches = (len(messages) + BATCH_SIZE - 1) // BATCH_SIZE  # Calculate total number of batches
    batches_per_channel = (num_batches + NUM_CHANNELS - 1) // NUM_CHANNELS  # Batches each channel will handle

    for i, channel in enumerate(channels):
        start_index = i * batches_per_channel * BATCH_SIZE
        end_index = min(start_index + batches_per_channel * BATCH_SIZE, len(messages))
        for j in range(start_index, end_index, BATCH_SIZE):
            batch = messages[j:j+BATCH_SIZE]
            send_message_batch(channel, batch, queue_name, i+1)
            # Delay between batches
            time.sleep(DELAY_BETWEEN_BATCHES)

def main():
    connection = None
    try:
        while True:
            try:
                # Establish RabbitMQ connection and channels
                connection, channels = establish_connection()

                # Define the SQL directory path
                sql_directory = ABSOLUTE_DIR
                
                # Parse SQL files and get messages
                messages = parse_sql_files(sql_directory)

                # Process batches with multiple channels
                process_batches(channels, messages, QUEUE_NAME)

                break  # Exit the loop if processing completes without errors

            except pika.exceptions.AMQPConnectionError as e:
                print(f"Connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    except KeyboardInterrupt:
        print('KeyboardInterrupt: Exiting gracefully.')
    except Exception as e:
        print(f'Error occurred: {str(e)}')
    finally:
        # Close RabbitMQ connection
        if connection:
            close_connection(connection)

if __name__ == '__main__':
    time.sleep(4)
    print("Done Snoozing")
    main()
