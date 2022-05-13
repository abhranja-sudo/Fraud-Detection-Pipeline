from os import path
from os import listdir
import os
from datetime import datetime
import json
import joblib
TRANSACTIONS_PATH = os.getcwd() + '/transactions'

def merge_valid_transactions_to_dataset() :
    transaction_list = []
    for files in listdir(TRANSACTIONS_PATH) :
        file = files.split('.')[0]
        date_time = datetime.strptime(file, "%Y-%m-%d")
        no_of_days = (date_time.now().date() - date_time.date()).days

        if no_of_days == 0 :
            f = open('transactions/'+ file + '.json','r+').read()

            if len(f) > 1 :
                data = json.loads(f)
                transaction_list += data

            f = open('transactions/'+str(date_time.now().date()) + '.csv', 'w+')
            print(len(transaction_list))

            for j in transaction_list :
                if j['prediction'] == 1 :
                    print(j)
            # print(transaction_list)
            json.dump(transaction_list, f)
            transaction_list = []


def check_testing_set () :
    testing_set = joblib.load('results.pkl')
    actual_testing_set = joblib.load('test_results.pkl')
    # producer_results = joblib.load('new_testing')
    import pandas as pd

    print(testing_set[testing_set['pred'] == 1])
    print(actual_testing_set[actual_testing_set['pred'] == 1])
    print(type(testing_set['TRANSACTION_ID'].to_list()[0]))
    print(type(actual_testing_set['TRANSACTION_ID'].to_list()[0]))
    print(testing_set.shape)
    print(actual_testing_set.shape)
    # testing_set['final_answer'] = np.where(testing_set['pred'] == actual_testing_set['pred'], 'True', 'False')
    count = 0

    testing_set = testing_set[testing_set['pred'] == 1]
    actual_testing_set = actual_testing_set[actual_testing_set['pred'] == 1]
    def func (series) :
        actual_series = None
        global count
        actual_series = actual_testing_set[actual_testing_set['TRANSACTION_ID'] == series['TRANSACTION_ID']]
        if actual_series.size == 0 :
            actual_series = None
        print(actual_series)
        if actual_series is not None and actual_series.iloc[0,:]['pred'] == series['pred'] :
            return 1

    a = testing_set.apply(func, axis =1 )
    print(a)

    # print(actual_testing_set[actual_testing_set['pred']==1].shape)
    # print(type(testing_set))
    # testing_set['json'] = testing_set.apply(lambda x: x.to_json(), axis=1)
    #
    # messages = testing_set.json.tolist()
    #
    # print(len(messages))

# merge_valid_transactions_to_dataset()
# check_testing_set()
# import json
# f = open('test_results.json','r').read()
# a = json.loads(f)


def check_testing_set_2 () :
    from pyspark.sql import SparkSession

    spark = (SparkSession
             .builder
             .master('local')
             .appName('TEST')
             .getOrCreate())
    producer = joblib.load('new_testing_set.pkl')
    answer  = json.loads(open('data/transactions/2022-05-12.json', 'r').read())
    import pandas as pd
    df = pd.DataFrame(answer)

    print(producer.shape)
    print(df.shape)
    # df = df.applymap(str)
    # from pyspark.ml.evaluation import BinaryClassificationEvaluator
    # newDf = spark.createDataFrame(df)
    # evaluator = BinaryClassificationEvaluator(labelCol="TX_FRAUD", rawPredictionCol="prediction")
    # newDf = newDf.to_pandas_on_spark()
    # newDf['TX_FRAUD'] = newDf['TX_FRAUD'].astype(float)
    # newDf['prediction'] = newDf['prediction'].astype(float)
    # print(newDf['prediction'])
    # accuracy = evaluator.evaluate(newDf.to_spark())
    # print("Accuracy = %s" % (accuracy))
    # print("Test Error = %s" % (1.0 - accuracy))
    # daataa = {
    #     'accuracy' : accuracy,
    #     'error' : (1.0 - accuracy)
    # }

    # def func (series) :
    #     actual_series = None
    #     global count
    #     print(series, producer.columns)
    #     actual_series = producer[producer['TRANSACTION_ID'] == series['TRANSACTION_ID']]
    #     if actual_series.size == 0 :
    #         actual_series = None
    #     print(actual_series)
    #     if actual_series is not None and actual_series.iloc[0,:]['prediction'] == series['prediction'] :
    #         return 1
    #
    # a = df.apply(func, axis =1 )

    print(df[df['TX_FRAUD'] == df['prediction']][['prediction','TRANSACTION_ID','TX_FRAUD']])

    # print(a)
    # print(producer)

check_testing_set_2()