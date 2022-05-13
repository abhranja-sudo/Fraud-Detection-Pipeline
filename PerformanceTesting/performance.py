import sys
import joblib
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket
import uuid
import json
import threading
from dotenv import load_dotenv
from datetime import datetime
from os import path
import os

load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': 'smallest'}

TOPIC_TRANSACTIONS = os.getenv('TOPIC_TRANSACTIONS')
TOPIC_PREDICTIONS = os.getenv('TOPIC_PREDICTIONS')

testing_set_results = {}
last_message_id = ''

testing_set = joblib.load('../new_testing_set.pkl')
testing_set['json'] = testing_set.apply(lambda x: x.to_json(), axis=1)

messages = testing_set.json.tolist()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    # else:
    #     print("Message produced: %s" % (str(msg)))

def start_producing():
    global started_at
    global testing_set_results
    global last_message_id

    started_at = datetime.now()

    producer = Producer(conf)

    for i in range(len(messages)):
        message_id = str(uuid.uuid4())

        if i == len(messages)-1 :
            last_message_id = message_id

        message = {'request_id': message_id, 'data': json.loads(messages[i])}

        d = json.loads(messages[i])
        d['request_id'] = message_id
        d['sent_request_at'] = datetime.now()
        testing_set_results[d['request_id']] = d

        producer.produce(TOPIC_TRANSACTIONS, json.dumps(message).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        producer.poll(0.009)

    print('PRODUCED ALL THE DATA', len(messages))

running = True
MIN_COMMIT_COUNT = 1
latest_file_datetime = None
latest_datetime = None
count = 0

def addPredictionToResults(message) :
    global testing_set_results
    global latest_file_datetime
    global count
    global started_at

    print(count)
    if message['request_id'] not in testing_set_results :
        return

    testing_set_results[message['request_id']]['prediction'] = 1 if message['fraud'] else 0
    testing_set_results[message['request_id']]['recieved_response_at'] = datetime.now()
    # testing_set_results[message['request_id']]['sent_request_at'] = str(testing_set_results[message['request_id']]['sent_request_at'])

    count += 1
    if message['request_id'] == last_message_id:
        diff = datetime.now() - started_at
        print('Throughput of the pipeline is', len(list(testing_set_results.keys()))/diff.total_seconds(),'requests/second')

        keys = list(testing_set_results.keys())
        latency = 0
        for i in range(3500,len(keys)) :
            key = keys[i]
            if 'recieved_response_at' in testing_set_results[key] :
                latency += (testing_set_results[key]['recieved_response_at'] -
                            testing_set_results[key]['sent_request_at']).total_seconds()
            if i == len(keys)-1:
                print((testing_set_results[key]['recieved_response_at'] - testing_set_results[key]['sent_request_at']).total_seconds())
        print('Average Latency' , latency/(len(keys)-3500))

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
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    # Load the testing set from the file
    threads = []
    t = threading.Thread(target=start_producing)
    t2 = threading.Thread(target=getPrediction)

    threads.append(t)
    threads.append(t2)
    t2.start()
    t.start()


