from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, LongType
from pyspark.ml.feature import Normalizer, StandardScaler
import random
import sys, socket, uuid, json, random, time, os, pyspark.pandas as ps
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pyspark.sql import functions as F
import time
import pyspark.pandas as ps
import pyspark.ml as ml
from datetime import datetime
from utilities import read_from_files, scaleData

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        }

producer_conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}


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
    classifier = classifier.load('/home/kshitij/Downloads/jupyterenv/Fraud-Detection-Pipeline/model_rfc/trained')

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    # else:
    #     print("Message produced: %s" % (str(msg)))

def msg_process(msg):

    producer = Producer(producer_conf)

    prediction = msg['prediction']
    if(prediction == 1):
        messageToSend = {'request_id': msg['request_id'], 'fraud': 'True'}
    else:
        messageToSend = {'request_id': msg['request_id'], 'fraud': 'False'}
    producer.produce('predictions', json.dumps(messageToSend).encode('utf-8'), callback=acked)
    producer.flush()

    # print("\033[1;31;40m -- PREDICTION: Sent message with id {}".format(msg['request_id']))
    # print(messageToSend)
    # producer.poll(1)


def get_prediction(df) :
    global classifier

    initial_features = input_features[:] + ['request_id']
    select_features = initial_features[:]
    select_features = select_features + ['rawPrediction',
                       'prediction', 'probability']
    df = df.to_spark().select(initial_features).to_pandas_on_spark()

    (train_df,df) = scaleData(df, df, input_features)
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

def send_message(row, batch_no) :
    global count
    count += 1
    print('Processing message for batch', batch_no)
    msg_process({"prediction": row['prediction'], "request_id": row['request_id']})
test_count = 0
test_count_2 = 0



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
        # .limit(1000)

    df = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # def test(row) :
    #     print(row)
    # df.apply(test,axis=1)
    print(type(df))
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("Zipcode", StringType(), True),
        StructField("ZipCodeType", StringType(), True),
        StructField("City", StringType(), True),
        StructField("TERMINAL_ID_RISK_1DAY_WINDOW", StringType(), True)
    ])
    dfJSON = df.withColumn("value", from_json(col("value"), schema)) \
        .select("value.*")

    posts_stream = dfJSON.writeStream.format("console").start().awaitTermination()


    # posts_stream = df.writeStream.trigger(processingTime="5 seconds").foreachBatch(func).start().awaitTermination()
