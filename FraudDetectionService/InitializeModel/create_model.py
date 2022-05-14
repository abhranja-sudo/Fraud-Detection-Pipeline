# General
import os
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import joblib
import time
import json
import datetime
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from utilities import read_from_files, scaleData
import pyspark.ml as ml
from pyspark.ml.feature import StringIndexer
from pyspark.pandas.config import set_option, reset_option

DIR_INPUT= '../../data/simulated-data-transformed'
PATH = Path('data/')
MODEL_PATH = os.path.dirname(os.getcwd())+ '/model_rfc/trained'
BEGIN_DATE = "2018-07-25"
END_DATE = "2018-08-14"

output_feature="TX_FRAUD"

input_features=['TX_AMOUNT','TX_DURING_WEEKEND', 'TX_DURING_NIGHT', 'CUSTOMER_ID_NB_TX_1DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW', 'CUSTOMER_ID_NB_TX_7DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW', 'CUSTOMER_ID_NB_TX_30DAY_WINDOW',
       'CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW', 'TERMINAL_ID_NB_TX_1DAY_WINDOW',
       'TERMINAL_ID_RISK_1DAY_WINDOW', 'TERMINAL_ID_NB_TX_7DAY_WINDOW',
       'TERMINAL_ID_RISK_7DAY_WINDOW', 'TERMINAL_ID_NB_TX_30DAY_WINDOW',
       'TERMINAL_ID_RISK_30DAY_WINDOW']

def initialize():
    print("Create data folder if it does not exist")
    (PATH).mkdir(exist_ok=True)

def get_train_test_set(transactions_df,
                       start_date_training,
                       delta_train=7, delta_delay=7, delta_test=7):

    transactions_df.TX_DATETIME = ps.to_datetime(transactions_df.TX_DATETIME)

    train_df = transactions_df[(transactions_df.TX_DATETIME >= start_date_training) &
                               (transactions_df.TX_DATETIME < start_date_training + datetime.timedelta(
                                   days=delta_train))]

    test_df = []

    known_defrauded_customers = set(train_df[train_df.TX_FRAUD == 1].CUSTOMER_ID.to_numpy())

    start_tx_time_days_training = train_df.TX_TIME_DAYS.min()

    for day in range(delta_test):
        test_df_day = transactions_df[transactions_df.TX_TIME_DAYS == start_tx_time_days_training +
                                      delta_train + delta_delay +
                                      day]

        test_df_day_delay_period = transactions_df[transactions_df.TX_TIME_DAYS == start_tx_time_days_training +
                                                   delta_train +
                                                   day - 1]

        new_defrauded_customers = set(test_df_day_delay_period[test_df_day_delay_period.TX_FRAUD == 1].CUSTOMER_ID.to_numpy())
        known_defrauded_customers = known_defrauded_customers.union(new_defrauded_customers)

        test_df_day = test_df_day[~test_df_day.CUSTOMER_ID.isin(known_defrauded_customers)]

        test_df.append(test_df_day)

    test_df = ps.concat(test_df)

    train_df = train_df.sort_values('TRANSACTION_ID')
    test_df = test_df.sort_values('TRANSACTION_ID')

    return (train_df, test_df)


def fit_model_and_get_predictions(train_df, test_df,
                                  input_features, output_feature="TX_FRAUD", scale=True):
    if scale:
        (train_df, test_df) = scaleData(train_df, test_df, input_features)

    start_time = time.time()

    stringIndexer = StringIndexer(inputCol=output_feature, outputCol='labelIndex')
    si_model = stringIndexer.fit(train_df)
    td = si_model.transform(train_df)

    classifier = ml.classification.RandomForestClassifier(featuresCol = 'feature', labelCol = 'labelIndex')

    classifier= classifier.fit(td)

    training_execution_time = time.time() - start_time

    start_time = time.time()

    si_model = stringIndexer.fit(test_df)
    td = si_model.transform(test_df)
    predictions_test = classifier.transform(td)

    select_features = input_features[:]
    select_features = select_features + ['rawPrediction',
                       'prediction', 'probability','TRANSACTION_ID',output_feature]


    evaluator = BinaryClassificationEvaluator(labelCol="labelIndex", rawPredictionCol="prediction")
    accuracy = evaluator.evaluate(predictions_test)

    print("Accuracy = %s" % (accuracy))
    print("Test Error = %s" % (1.0 - accuracy))

    performance_metric = {
        'accuracy' : str(accuracy*100) + '%',
        'error' : str((1.0 - accuracy)*100) + '%'
    }

    f = open('performance_metric' + '.json', "w+")
    json.dump(performance_metric, f)
    f.close()

    predictions_test = ((predictions_test.select(select_features)).to_pandas_on_spark())
    prediction_execution_time = time.time() - start_time

    testing_set_to_write = create_testing_set(predictions_test)

    joblib.dump(testing_set_to_write.to_pandas(), '../../new_testing_set.pkl')

    predictions_train = classifier.transform(train_df)
    predictions_train = ((predictions_train.select(select_features)).to_pandas_on_spark())

    model_and_predictions_dictionary = {'classifier': classifier,
                                        'predictions_test': predictions_test,
                                        'predictions_train': predictions_train,
                                        'training_execution_time': training_execution_time,
                                        'prediction_execution_time': prediction_execution_time
                                        }

    return model_and_predictions_dictionary

def create_testing_set(test_df) :
    fraud_df = test_df[test_df['prediction'] == 1]
    non_fraud_df = test_df[test_df['prediction'] == 0 ].iloc[:15000,:]
    final_df = ps.concat([fraud_df,non_fraud_df])
    return final_df

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Assignment3").getOrCreate()

    transactions_df=read_from_files(DIR_INPUT, BEGIN_DATE, END_DATE)

    # Training period
    start_date_training = datetime.datetime.strptime("2018-07-25", "%Y-%m-%d")
    delta_train = delta_delay = delta_test = 7

    end_date_training = start_date_training + datetime.timedelta(days=delta_train - 1)

    # Test period
    start_date_test = start_date_training + datetime.timedelta(days=delta_train + delta_delay)
    end_date_test = start_date_training + datetime.timedelta\
        (days=delta_train + delta_delay + delta_test - 1)
    (train_df, test_df) = get_train_test_set(transactions_df, start_date_training, delta_train=7,
                                             delta_delay=7, delta_test=7)

    model_and_predictions_dictionary = fit_model_and_get_predictions(train_df, test_df, input_features,
                                                                     output_feature, scale=True)


    model_and_predictions_dictionary['classifier'].write()\
        .overwrite()\
        .save(MODEL_PATH)

    set_option("compute.ops_on_diff_frames", True)
    test_df['pred'] = model_and_predictions_dictionary['predictions_test']['prediction']

    print('ML model has been created')

    #Write test results to a file
    # results = open('../test_results.json', "w+")
    # joblib.dump((test_df.to_pandas()), 'test_results.pkl')
    #
    # results.close()

