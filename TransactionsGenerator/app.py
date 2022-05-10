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

testing_set_results = []
last_message_id = ''
testing_set = joblib.load('new_testing_set.pkl')
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
    for i in range(len(messages)):
        message_id = str(uuid.uuid4())

        if i == len(messages)-1 :
            last_message_id = message_id

        message = {'request_id': message_id, 'data': json.loads(messages[i])}

        d = json.loads(messages[i])
        d['request_id'] = message_id

        testing_set_results.append(d)

        producer.produce(TOPIC_TRANSACTIONS, json.dumps(message).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        # producer.poll(1)
        # producer.poll(1)

    print('PRODUCED ALL THE MOTHERFUCKING DATA')

running = True
MIN_COMMIT_COUNT = 55000
latest_file_datetime = None
latest_datetime = None
from datetime import datetime
from os import path


def addPredictionToResults(message) :
    global testing_set_results
    global latest_file_datetime

    for index,test in enumerate(testing_set_results) :
        if test['request_id'] == message['request_id'] :
            testing_set_results[index]['prediction'] = message['fraud']

            latest_file_datetime = str(datetime.now().date())


            if path.isfile('transactions/' + latest_file_datetime + ".json"):
                to_dump = json.loads(open('transactions/' + latest_file_datetime + '.json', "r+").read())
            else:
                to_dump = []

            to_dump.append(testing_set_results[index])

            f = open('transactions/' + latest_file_datetime + '.json', "w+")
            json.dump(to_dump, f)
            f.close()

            break



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
                addPredictionToResults(message)
                print("\033[1;31;40m Received message with id {} and fraud = {}".format(message['request_id'], message['fraud']))
                # msg_process(msg)
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
    t.start()
    t2.start()

