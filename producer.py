from confluent_kafka import Producer
conf = {
    'bootstrap.servers': 'kafka-0:9092,kafka-1:9092' 
    }
producer = Producer(conf)
def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery failed:{err}")
    else:
        print(f"Message delivered to{msg.topic()}[{msg.partition()}]")
for i in range(10):
    message=f'Hello Kafka{i}'
    producer.produce('test',message.encode('utf-8'),callback=delivery_report)
    producer.poll(0) # Triggerdeliverycallback
producer.flush() # Waitforallmessagesto bedeliveredcallback