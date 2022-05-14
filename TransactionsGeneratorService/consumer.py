import sys
import joblib
from confluent_kafka import Consumer, KafkaError, KafkaException
import socket
import json
import os
from dotenv import load_dotenv
import time
import math
load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': os.getenv('GROUP_ID'),
        'auto.offset.reset': 'smallest'}

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

running = True
MIN_COMMIT_COUNT = 1
latest_file_datetime = None
latest_datetime = None
count = 0

def addPredictionToResults(message, recieved_at) :
    global testing_set_results
    global latest_file_datetime
    global last_message_id
    global count

    transaction = message["data"]
    transaction["recieved_at"] = recieved_at
    testing_set_results.append(transaction)
    latency = 0
    if transaction['is_last_message'] :
        diff = recieved_at - testing_set_results[0]['sent_request_at']
        print(diff,len(testing_set_results))

        print('Throughput', len(testing_set_results)/diff)

        for i in range(3500,len(testing_set_results)) :
            result = testing_set_results[i]
            if 'recieved_at' in result :
                latency += (result['recieved_at'] -
                            result['sent_request_at'])
            if i == len(testing_set_results)-1:
                print((result['recieved_at'] - result['sent_request_at']))
        print('Average Latency' , latency/(len(testing_set_results)-3500))
    # if message['request_id'] == last_message_id :
    #     f = open('../data/transactions/' + latest_file_datetime + '.json', "w+")
    #     json.dump(testing_set_results, f)
    #     f.close()

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
                addPredictionToResults(message, math.ceil(time.time()))

                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    except Exception as e  :
        print('Consumer failed with error =',e)
    finally:
        consumer.close()

if __name__ == '__main__':
    getPrediction()

