from confluent_kafka import Consumer
from kafka_producer import TwitterStreamListener
import json
################
cons = Consumer({'bootstrap.servers': 'localhost:9092',
                 'group.id': 'python-consumer',
                 'auto.offset.reset': 'earliest'})

print('Kafka Consumer has been initiated...')
print('Available topics to consume: ', cons.list_topics().topics)
cons.subscribe(['bola'])


################

def main():

    while True:
        msg = cons.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data = msg.value().decode('utf-8')
        print(data)
    cons.close()

print("hello")

if __name__ == '__main__':
    main()
