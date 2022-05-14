import sys
import joblib
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket
import uuid
import json
import threading
from datetime import datetime
from os import path
import os
from dotenv import load_dotenv
import time
import math

load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': 'latest'}

TOPIC_TRANSACTIONS = os.getenv('TOPIC_TRANSACTIONS')
TOPIC_PREDICTIONS = os.getenv('TOPIC_PREDICTIONS')

testing_set_results = []
last_message_id = ''

testing_set = joblib.load('../new_testing_set.pkl')
testing_set['json'] = testing_set.apply(lambda x: x.to_json(), axis=1)
messages = testing_set.json.tolist()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def start_producing():
    producer = Producer(conf)
    global testing_set_results
    global last_message_id

    # started_at = datetime.now()

    for i in range(len(messages)):
        message_id = str(uuid.uuid4())
        json_object = json.loads(messages[i])
        json_object['is_last_message'] = False

        if i == len(messages)-1 :
            last_message_id = message_id
            json_object['is_last_message'] = True

        json_object['sent_request_at'] = math.ceil(time.time())
        message = {'request_id': message_id, 'data': json_object}

        training_set_json = json.loads(messages[i])
        training_set_json['request_id'] = message_id

        testing_set_results.append(training_set_json)

        producer.produce(TOPIC_TRANSACTIONS, json.dumps(message).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))

        producer.poll(0.015)

    print('PRODUCED THE DATA')

running = True
MIN_COMMIT_COUNT = 1
latest_file_datetime = None
latest_datetime = None
count = 0


def addPredictionToResults(message) :
    global testing_set_results
    global latest_file_datetime
    global last_message_id
    global count

    for index,test in enumerate(testing_set_results) :
        if test['request_id'] == message['request_id'] :
            testing_set_results[index]['prediction'] = 1 if message['fraud'] else 0

            latest_file_datetime = str(datetime.now().date())
            count += 1
            break

    if message['request_id'] == last_message_id :
        f = open('../data/transactions/' + latest_file_datetime + '.json', "w+")
        json.dump(testing_set_results, f)
        f.close()

def getPrediction():
    consumer = Consumer(conf)
    try:
        consumer.subscribe([TOPIC_PREDICTIONS])
        msg_count = 0
        while running:
            msg = consumer.poll()
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
                if 'fraud' not in message :
                    continue
                print("\033[1;31;40m Received message with id {} and fraud = {}".format(message['request_id'], message['fraud']))

                addPredictionToResults(message)

                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    except Exception as e  :
        print('Consumer failed with error =',e)
    finally:
        consumer.close()

if __name__ == '__main__':
    start_producing()

