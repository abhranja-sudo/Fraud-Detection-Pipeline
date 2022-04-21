import joblib
from confluent_kafka import Producer
import socket

import uuid
import json
import threading


KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname()}

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def start_producing(messages):
    producer = Producer(conf)
    for i in range(len(messages)):
        message_id = str(uuid.uuid4())
        message = {'request_id': message_id, 'data': json.loads(messages[i])}

        producer.produce('events', json.dumps(message).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        producer.poll(3)


if __name__ == '__main__':
    # Load the testing set from the file
    testing_set = joblib.load('testing_set.pkl')
    testing_set['json'] = testing_set.apply(lambda x: x.to_json(), axis=1)
    messages = testing_set.json.tolist()
    threads = []
    t = threading.Thread(target=start_producing(messages))
    threads.append(t)
    t.start()