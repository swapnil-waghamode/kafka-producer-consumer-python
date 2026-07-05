
# importing consumer

from confluent_kafka import Consumer
import json

'''
auto.offset.reset 

- what to do when there is no initial offset in kafka or if the current offset does not exist 
  anymore on the server  (eg. because data has been deleted )

* earliest : automatically reset the offst to earliest offset 

* latest : automatically reset the offst to latest offset 

* by_duration : <duration> : automatically reset the offst to configured <duration>
               from current time-stamp

* none : throw exception to consumer when no offset found for consumer's group
'''

# First will connect to broker so that consumer can listens to the upcoming messages
# Once message comes in will process them one by one

consumer_config = {
    'bootstrap.servers':'localhost:9092',
    'group.id': 'order-tracker', # group id identifies consumer group the consumer belongs to
    "auto.offset.reset" : "earliest"
}

# Creatinng consumer object
consumer = Consumer(consumer_config)


# subscribing to the topics

consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to order topic")

'''
consumer.poll() : 
  Asks broker if there is new message in subscribed topic and returns it to consumer for processing

'''

# Consumer message and errors logic
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Error :  {msg.error()}")
            continue

        # converting it from byte string format
        value = msg.value().decode("utf-8")

        # converting from json string to dictionary
        order = json.loads(value)

        print(f"📩 Messgage received : {order['quantity']} X {order['item']} from {order['user']}")

except KeyboardInterrupt:
    print(f"🔴 Stopping cosumer")

finally:
    consumer.close()

'''
consumer.close() :  is crucial for properly resources associted with consumer instance

- Ensures that networks connections, file handles are closed, offsets are commited and consumer 
  partitions assignments are revoked

'''




