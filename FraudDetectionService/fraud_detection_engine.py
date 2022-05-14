from pyspark.sql import SparkSession
import socket, json, os
from confluent_kafka import Producer
import pyspark.pandas as ps
import pyspark.ml as ml
from datetime import datetime
from utilities import scaleData
from dotenv import load_dotenv

load_dotenv()

KAFKA_HOST = os.getenv('KAFKA_HOST')

producer_conf = {'bootstrap.servers': KAFKA_HOST,
}


TOPIC_TRANSACTIONS = "transactions"
TOPIC_PREDICTIONS = 'predictions'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

model_loaded_date = None
classifier = None
count = 0


input_features=['TX_AMOUNT','TX_DURING_WEEKEND', 'TX_DURING_NIGHT', 'CUSTOMER_ID_NB_TX_1DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW', 'CUSTOMER_ID_NB_TX_7DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW', 'CUSTOMER_ID_NB_TX_30DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW', 'TERMINAL_ID_NB_TX_1DAY_WINDOW',
       'TERMINAL_ID_RISK_1DAY_WINDOW', 'TERMINAL_ID_NB_TX_7DAY_WINDOW',
       'TERMINAL_ID_RISK_7DAY_WINDOW', 'TERMINAL_ID_NB_TX_30DAY_WINDOW',
       'TERMINAL_ID_RISK_30DAY_WINDOW']


def load_new_model() :
    global classifier
    classifier = ml.classification.RandomForestClassificationModel()
    classifier = classifier.load(os.getcwd() + '/model_rfc/trained')

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def msg_process(msg):

    producer = Producer(producer_conf)

    prediction = msg['prediction']
    data = msg['data']
    del data['rawPrediction']
    del data['probability']
    if(prediction == 1):
        messageToSend = {'request_id': msg['request_id'],'data':data ,'fraud': 'True'}
    else:
        messageToSend = {'request_id': msg['request_id'], 'data':data ,'fraud': 'False'}

    producer.produce('predictions', json.dumps(messageToSend).encode('utf-8'), callback=acked)
    producer.flush()

    print("\033[1;31;40m -- PREDICTION: Sent message with id {}".format(msg['request_id']))
    producer.poll(0.009)


def get_prediction(df) :
    global classifier

    initial_features = input_features[:] + ['request_id','sent_request_at','is_last_message']
    select_features = initial_features[:]
    select_features = select_features + ['rawPrediction',
                       'prediction', 'probability']

    df = df.to_spark().select(initial_features).to_pandas_on_spark()

    df = scaleData(df, input_features)
    predictions_test = classifier.transform(df)
    predictions_test = ((predictions_test.select(select_features)).to_pandas_on_spark())
    return predictions_test


def convert_string_to_json(row) :
    request = json.loads(row)
    data = request['data']
    data['request_id'] = request['request_id']
    return data

def create_df(batch_df) :
    value_series = batch_df['value'].to_list()
    value_list = []
    for value in value_series :
        value_list.append(convert_string_to_json(value))
    df = ps.DataFrame(value_list)
    return df

def send_message(row) :
    global count
    count += 1
    msg_process({"prediction": row['prediction'], "request_id": row['request_id'], "data" : row.to_dict()})

total_events = 0

def func(batch_df, batch_id) :
    global model_loaded_date
    global count
    global total_events

    current_date = datetime.now().date()

    if model_loaded_date is None or (current_date - model_loaded_date).days > 0 :
        load_new_model()
        model_loaded_date = datetime.now().date()

    total_events += batch_df.to_pandas_on_spark()['value'].size

    if batch_df.to_pandas_on_spark()['value'].size > 0 :
        input_df = create_df(batch_df.to_pandas_on_spark())
        results_df = get_prediction(input_df)
        results_df.apply(send_message,axis=1)

    print(count)
    print(total_events)

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .master('local')
             .appName('FraudLoginDetection')
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
             .getOrCreate())

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("subscribe", TOPIC_TRANSACTIONS) \
        .load()\

    df = df.selectExpr("CAST(value AS STRING)", "timestamp")

    posts_stream = df.writeStream.foreachBatch(func).start().awaitTermination()
