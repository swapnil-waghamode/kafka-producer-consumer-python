# ============================================================
# Import required libraries
# ============================================================

# Producer is used to publish (send) messages to Kafka topics.
from confluent_kafka import Producer

# uuid is used to generate a unique order ID.
import uuid

# json is used to convert Python dictionaries into JSON strings.
import json


# ============================================================
# Kafka Producer Configuration
# ============================================================

"""
bootstrap.servers

This specifies one or more Kafka brokers that the producer uses as the initial connection point.

The producer connects to these brokers and automatically discovers the remaining brokers 
in the Kafka cluster.

Even if your cluster contains multiple brokers, providing one reachable broker is usually enough.

Example:
localhost:9092
"""

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

# Create a Kafka Producer object.
producer = Producer(producer_config)


# ============================================================
# Delivery Callback Function
# ============================================================

"""
This callback is executed after Kafka attempts to deliver a message.

It is called asynchronously when producer.flush() or producer.poll() processes the delivery events.

Parameters
----------
err : Exception or None
    Contains the error if message delivery fails.

msg : Message
    Contains metadata about the delivered message.
"""

def delivery_report(err, msg):

    if err:
        print(f"❌ Message delivery failed: {err}")

    else:
        print(f"✅ Delivered Message: {msg.value().decode('utf-8')}")
        print(
            f"✅ Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


# ============================================================
# Create Sample Event (Message)
# ============================================================

"""
Kafka messages are simply bytes.

Here we create a Python dictionary representing an order.
In a real-world application, this data could come from:

- REST APIs
- Web applications
- Mobile applications
- Payment services
- Inventory systems
"""

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Elon Musk",
    "item": "Chiken burger",
    "quantity": 1
}


# ============================================================
# Convert Python Dictionary to Bytes
# ============================================================

"""
Kafka producers send messages as bytes.

Step 1:
Convert the Python dictionary into a JSON string.

Example:
{
    "user": "Swapnil"
}

becomes

'{"user":"Swapnil"}'

Step 2:
Encode the JSON string into UTF-8 bytes because Kafka
transmits byte data.

Result:
b'{"user":"Swapnil"}'
"""

value = json.dumps(order).encode("utf-8")


# ============================================================
# Publish Message to Kafka
# ============================================================

"""
produce() adds the message to the producer's internal buffer.

Parameters:

topic
    Kafka topic where the message will be sent.

value
    Message payload (must be bytes).

callback
    Function invoked after Kafka acknowledges success
    or reports failure.

Note:
produce() is asynchronous. It queues the message for
sending and returns immediately.
"""

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)


# ============================================================
# Flush Pending Messages
# ============================================================

"""
flush() blocks until all queued messages are delivered
(or until the timeout expires).

Without flush(), the program may terminate before Kafka
has actually sent the messages.

Use flush():
- before application shutdown
- after sending a batch of messages
"""

producer.flush()


# ============================================================
# Useful Kafka Docker Commands
# ============================================================

"""
List all Kafka topics
---------------------

docker exec -it kafka \
kafka-topics \
--bootstrap-server localhost:9092 \
--list


Create a new topic
------------------

docker exec -it kafka \
kafka-topics \
--bootstrap-server localhost:9092 \
--create \
--topic orders \
--partitions 3 \
--replication-factor 1


Describe a topic
----------------

docker exec -it kafka \
kafka-topics \
--bootstrap-server localhost:9092 \
--describe \
--topic orders


Consume all messages from the beginning
---------------------------------------

docker exec -it kafka \
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders \
--from-beginning


Delete a topic (if enabled)
---------------------------

docker exec -it kafka \
kafka-topics \
--bootstrap-server localhost:9092 \
--delete \
--topic orders
"""
