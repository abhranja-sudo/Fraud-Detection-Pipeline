import json
import socket
import joblib
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import json
import pandas as pd


# Load model
trained_model = joblib.load('trained_model.pkl')

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

TOPIC_TRANSACTION = 'transactions'
TOPIC_PREDICTION = 'predictions'

consumer = Consumer(conf)
producer = Producer(conf)

running = True
MIN_COMMIT_COUNT = 20

output_feature="TX_FRAUD"

input_features=['TX_AMOUNT','TX_DURING_WEEKEND', 'TX_DURING_NIGHT', 'CUSTOMER_ID_NB_TX_1DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW', 'CUSTOMER_ID_NB_TX_7DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW', 'CUSTOMER_ID_NB_TX_30DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW', 'TERMINAL_ID_NB_TX_1DAY_WINDOW',
       'TERMINAL_ID_RISK_1DAY_WINDOW', 'TERMINAL_ID_NB_TX_7DAY_WINDOW',
       'TERMINAL_ID_RISK_7DAY_WINDOW', 'TERMINAL_ID_NB_TX_30DAY_WINDOW',
       'TERMINAL_ID_RISK_30DAY_WINDOW']


def msg_process(msg):
    message = msg.value()
    message = json.loads(message)
    prediction = predict(trained_model, message)[0]
    if(prediction == 1):
        messageToSend = {'request_id': message['request_id'], 'fraud': 'True'}
    else:
        messageToSend = {'request_id': message['request_id'], 'fraud': 'False'}

    producer.produce(TOPIC_PREDICTION, json.dumps(messageToSend).encode('utf-8'), callback=acked)
    producer.flush()

    print("\033[1;31;40m -- PREDICTION: Sent message with id {}".format(message['request_id']))

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
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
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def predict(model, msg):
    msg = pd.json_normalize(msg['data'])
    return model.predict((msg[input_features]))

if __name__ == '__main__':
        consume_loop(consumer, [TOPIC_TRANSACTION])