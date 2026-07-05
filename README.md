# 🚀 Kafka Producer-Consumer with Python

A beginner-friendly project demonstrating **event-driven communication** using **Apache Kafka**, **Python**, and **Docker**. This project simulates an order processing system where a **Producer** publishes order events to a Kafka topic, and a **Consumer** subscribes to that topic to process incoming events in real time.

---

# 📌 Project Overview

Modern distributed applications often communicate **asynchronously** instead of making direct API calls.

This project demonstrates how **Apache Kafka** acts as a message broker between independent applications.

**Workflow:**

```
Producer  ➜  Kafka Topic  ➜  Consumer
```

The producer generates an order event, Kafka stores it inside a topic, and the consumer continuously listens for and processes new events.

---

# ✨ Features

- Kafka Producer implementation in Python
- Kafka Consumer implementation in Python
- Apache Kafka running with Docker Compose
- KRaft Mode (No ZooKeeper Required)
- JSON Serialization & Deserialization
- Consumer Groups
- Offset Management
- Delivery Report Callback
- Real-Time Event Processing
- Beginner-Friendly Project Structure

---

# 🛠️ Tech Stack

| Technology | Purpose |
|------------|---------|
| Python 3.x | Producer & Consumer |
| Apache Kafka | Event Streaming Platform |
| Confluent Kafka Python | Kafka Python Client |
| Docker | Kafka Container |
| Docker Compose | Container Orchestration |
| JSON | Message Serialization |
| UUID | Unique Order ID Generation |

---

# 📂 Project Structure

```text
kafka-producer-consumer-python/
│
├── producer.py
├── consumer.py
├── docker-compose.yaml
├── requirements.txt
├── README.md
└── .gitignore
```

---

# 🏗️ System Architecture

```text
                +----------------------+
                |    producer.py       |
                |  Creates Order Event |
                +----------+-----------+
                           |
                           |
                     Produce Message
                           |
                           ▼
                  +-------------------+
                  |   Apache Kafka    |
                  |    Topic: orders  |
                  +-------------------+
                           |
                           |
                     Consumer Poll
                           |
                           ▼
                +----------------------+
                |    consumer.py       |
                | Process Order Event  |
                +----------------------+
```

---

# 🔄 Project Workflow

## Step 1 - Start Kafka

Kafka Broker starts using Docker Compose in **KRaft Mode**.

```bash
docker compose up -d
```

---

## Step 2 - Create an Order Event

The Producer creates a Python dictionary.

```python
order = {
    "order_id": "...",
    "user": "Elon Musk",
    "item": "Chicken Burger",
    "quantity": 1
}
```

---

## Step 3 - Serialize the Message

The dictionary is converted into JSON and then encoded into bytes.

```
Python Dictionary
        │
        ▼
JSON String
        │
        ▼
UTF-8 Bytes
```

Kafka stores and transmits messages as **bytes**.

---

## Step 4 - Publish Message

The producer sends the message to the **orders** topic.

```python
producer.produce(
    topic="orders",
    value=value
)
```

Since Kafka Producer is asynchronous, the message is first stored inside the producer's internal buffer.

---

## Step 5 - Flush Messages

```python
producer.flush()
```

`flush()` ensures that every queued message is successfully delivered before the application exits.

---

## Step 6 - Subscribe to Topic

The consumer joins the **order-tracker** consumer group.

```python
consumer.subscribe(["orders"])
```

---

## Step 7 - Poll Messages

The consumer continuously asks Kafka for new messages.

```python
msg = consumer.poll(timeout=1.0)
```

The consumer waits **up to one second** for a message and immediately returns once a message arrives.

---

## Step 8 - Deserialize Message

The received bytes are converted back into a Python dictionary.

```
Bytes
   │
   ▼
UTF-8 String
   │
   ▼
JSON String
   │
   ▼
Python Dictionary
```

---

## Step 9 - Process Event

The consumer extracts the order details.

Example Output

```text
📩 Message received:
1 X Chicken Burger from Elon Musk
```

---

# 📚 Kafka Concepts Covered

- Apache Kafka
- Producer
- Consumer
- Topic
- Consumer Groups
- Offset Management
- auto.offset.reset
- Message Serialization
- Message Deserialization
- Delivery Callback
- Event Streaming
- KRaft Mode
- Asynchronous Communication

---

# 📷 Sample Output

### Producer

```text
✅ Delivered Message:
{"order_id":"...","user":"Elon Musk","item":"Chicken Burger","quantity":1}

Topic : orders
Partition : 0
Offset : 15
```

### Consumer

```text
📩 Message received:
1 X Chicken Burger from Elon Musk
```

---

# 🔍 Useful Kafka Commands

### List Topics

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

### Describe Topic

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--describe \
--topic orders
```

### Read Messages

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders \
--from-beginning
```

### Stop Kafka

```bash
docker compose down
```

---

# 🎯 Learning Outcomes

After completing this project, you will understand:

- How Kafka Producers publish messages
- How Kafka Consumers receive messages
- How Kafka Topics store events
- How Consumer Groups work
- Offset Management
- JSON Serialization & Deserialization
- Event-Driven Architecture
- Running Kafka locally using Docker Compose
- Kafka KRaft Mode (Without ZooKeeper)

---

# 👨‍💻 Author

**Swapnil Waghamode**

GitHub: https://github.com/swapnil-waghamode
