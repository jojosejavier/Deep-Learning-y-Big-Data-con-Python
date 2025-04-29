# ========================================================================================
# This script generates and sends random comment messages to a Kafka topic named 'comment'.
# It includes a predefined list of comments and generates a random user_id for each message. 
# The messages are serialized into JSON format before being sent to Kafka. The producer runs in 
# an infinite loop, sending one message at a time with a random sleep time between 0.5 and 3 seconds. 
# The producer can be stopped manually with a KeyboardInterrupt.
# ========================================================================================

from kafka import KafkaProducer
import json
import random
import time

# === Configure Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON format
)

# === Predefined list of comments ===
comments_list = [
    "I love this product!",
    "This is terrible. I hate it.",
    "Great service, very satisfied!",
    "Not worth the price.",
    "Excellent quality, will buy again!",
    "Nor like or dislike"
]

# === Function to generate random user_id ===
def generate_user_id():
    """Generate a random user_id."""
    return f"user_{random.randint(1, 100)}"

# === Function to generate a comment message ===
def generate_message():
    """Generate a message with all required data."""
    message = {
        "user_id": generate_user_id(),  
        "comment": random.choice(comments_list)
    }
    return message

# === Loop to send comments randomly ===
try:
    while True:
        # Generate a message and send it
        message = generate_message()
        producer.send('comment', value=message)
        print(f"Comment sent: {message}")

        # Random sleep between 0.5 and 3 seconds
        time.sleep(random.uniform(0.5, 3))

except KeyboardInterrupt:
    print("Producer stopped manually.")

# === Finalize producer ===
producer.flush()  # Ensure all messages are sent
producer.close()  # Close the connection
