import sys

import joblib
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket

import uuid
import json
import threading


KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

TOPIC_TRANSACTIONS = 'transactions'
TOPIC_PREDICTIONS = 'predictions'

testing_set = joblib.load('testing_set.pkl')
testing_set['json'] = testing_set.apply(lambda x: x.to_json(), axis=1)
messages = testing_set.json.tolist()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def start_producing():
    producer = Producer(conf)
    for i in range(len(messages)):
        message_id = str(uuid.uuid4())
        message = {'request_id': message_id, 'data': json.loads(messages[i])}

        producer.produce(TOPIC_TRANSACTIONS, json.dumps(message).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        producer.poll(1)

running = True
MIN_COMMIT_COUNT = 20
def getPrediction():
    consumer = Consumer(conf)
    try:
        consumer.subscribe([TOPIC_PREDICTIONS])
        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                message = msg.value()
                message = json.loads(message)
                print("\033[1;31;40m Received message with id {} and fraud = {}".format(message['request_id'], message['fraud']))
                # msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    # Load the testing set from the file
    threads = []
    t = threading.Thread(target=start_producing)
    t2 = threading.Thread(target=getPrediction)

    threads.append(t)
    threads.append(t2)
    t.start()
    t2.start()