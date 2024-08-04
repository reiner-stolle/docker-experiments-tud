import pika
import json
import threading
import time

# Configuration for RabbitMQ
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
QUEUE_NAME = 'hello'
NUM_PROCESSORS = 3  # Number of processors
PREFETCH_COUNT = 10  # Number of messages to prefetch

# Global variable to keep track of the last processor used
last_processor = 0

# Function to get the next processor in a round-robin fashion
def get_next_processor():
    global last_processor
    processor_id = (last_processor % NUM_PROCESSORS) + 1
    last_processor += 1
    return processor_id

# Function to process a message
def process_message(ch, method, properties, body):
    try:
        message = json.loads(body)
    except json.JSONDecodeError:
        print(f"Failed to decode message: {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Check if message has already been dispatched
    if message.get('Dispatched', False):
        print(f"[IGNORING] Message already dispatched, ignoring: {message['QID']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Assign the message to the next processor in round-robin fashion
    processor_id = get_next_processor()
    message['Processor'] = processor_id
    message['Dispatched'] = True
    
    print(f"[LOGIC] Message with QID: {message['QID']} dispatched to processor {processor_id}")

    # Publish the updated message back to the RabbitMQ queue
    ch.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(message)
    )
    
    # Acknowledge the original message to remove it from the queue
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Function to consume messages
def consume_message():
    try:
        # Connection to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        
        # Define the channel
        channel = connection.channel()

        # Set prefetch count
        channel.basic_qos(prefetch_count=PREFETCH_COUNT)

        # Declare the queue with queue name as parameter
        channel.queue_declare(queue=QUEUE_NAME)

        # If message is received, call the callback function
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message, auto_ack=False)

        print('[DISPATCHER] Waiting for messages. To exit press CTRL+C')
        
        # Start consuming messages
        channel.start_consuming()

    except KeyboardInterrupt:
        print('KeyboardInterrupt: Exiting gracefully.')
    except Exception as e:
        print(f'Error occurred: {str(e)}')

def main():
    # Use threading to handle message consumption
    threads = []
    for _ in range(NUM_PROCESSORS):
        thread = threading.Thread(target=consume_message)
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    time.sleep(4)
    main()
