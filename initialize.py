# General
import os
from pathlib import Path

import pandas as pd
import numpy as np
import math
import sys
import joblib
import time
import pickle
import json
import datetime
import random
# Load data from the 2018-07-25 to the 2018-08-14
from utilities import read_from_files, scaleData
import sklearn
from sklearn import *

DIR_INPUT='/Users/ranjan/Downloads/simulated-data-transformed/'
PATH = Path('data/')
MODELS_PATH = PATH/'models'

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
    print("creating directory structure...")
    (PATH).mkdir(exist_ok=True)
    (MODELS_PATH).mkdir(exist_ok=True)

def get_train_test_set(transactions_df,
                       start_date_training,
                       delta_train=7, delta_delay=7, delta_test=7):
    # Get the training set data
    train_df = transactions_df[(transactions_df.TX_DATETIME >= start_date_training) &
                               (transactions_df.TX_DATETIME < start_date_training + datetime.timedelta(
                                   days=delta_train))]

    # Get the test set data
    test_df = []

    # Note: Cards known to be compromised after the delay period are removed from the test set
    # That is, for each test day, all frauds known at (test_day-delay_period) are removed

    # First, get known defrauded customers from the training set
    known_defrauded_customers = set(train_df[train_df.TX_FRAUD == 1].CUSTOMER_ID)

    # Get the relative starting day of training set (easier than TX_DATETIME to collect test data)
    start_tx_time_days_training = train_df.TX_TIME_DAYS.min()

    # Then, for each day of the test set
    for day in range(delta_test):
        # Get test data for that day
        test_df_day = transactions_df[transactions_df.TX_TIME_DAYS == start_tx_time_days_training +
                                      delta_train + delta_delay +
                                      day]

        # Compromised cards from that test day, minus the delay period, are added to the pool of known defrauded customers
        test_df_day_delay_period = transactions_df[transactions_df.TX_TIME_DAYS == start_tx_time_days_training +
                                                   delta_train +
                                                   day - 1]

        new_defrauded_customers = set(test_df_day_delay_period[test_df_day_delay_period.TX_FRAUD == 1].CUSTOMER_ID)
        known_defrauded_customers = known_defrauded_customers.union(new_defrauded_customers)

        test_df_day = test_df_day[~test_df_day.CUSTOMER_ID.isin(known_defrauded_customers)]

        test_df.append(test_df_day)

    test_df = pd.concat(test_df)

    # Sort data sets by ascending order of transaction ID
    train_df = train_df.sort_values('TRANSACTION_ID')
    test_df = test_df.sort_values('TRANSACTION_ID')

    joblib.dump(test_df, 'testing_set.pkl')

    return (train_df, test_df)


def fit_model_and_get_predictions(classifier, train_df, test_df,
                                  input_features, output_feature="TX_FRAUD", scale=True):
    # By default, scales input data
    if scale:
        (train_df, test_df) = scaleData(train_df, test_df, input_features)

    # We first train the classifier using the `fit` method, and pass as arguments the input and output features
    start_time = time.time()
    classifier.fit(train_df[input_features], train_df[output_feature])
    training_execution_time = time.time() - start_time

    # We then get the predictions on the training and test data using the `predict_proba` method
    # The predictions are returned as a numpy array, that provides the probability of fraud for each transaction
    start_time = time.time()
    predictions_test = classifier.predict_proba(test_df[input_features])[:, 1]
    prediction_execution_time = time.time() - start_time

    predictions_train = classifier.predict_proba(train_df[input_features])[:, 1]

    # The result is returned as a dictionary containing the fitted models,
    # and the predictions on the training and test sets
    model_and_predictions_dictionary = {'classifier': classifier,
                                        'predictions_test': predictions_test,
                                        'predictions_train': predictions_train,
                                        'training_execution_time': training_execution_time,
                                        'prediction_execution_time': prediction_execution_time
                                        }

    return model_and_predictions_dictionary

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # initialize()

    transactions_df=read_from_files(DIR_INPUT, BEGIN_DATE, END_DATE)
    # Training period
    start_date_training = datetime.datetime.strptime("2018-07-25", "%Y-%m-%d")
    delta_train = delta_delay = delta_test = 7

    end_date_training = start_date_training + datetime.timedelta(days=delta_train - 1)

    # Test period
    start_date_test = start_date_training + datetime.timedelta(days=delta_train + delta_delay)
    end_date_test = start_date_training + datetime.timedelta(days=delta_train + delta_delay + delta_test - 1)

    (train_df, test_df) = get_train_test_set(transactions_df, start_date_training, delta_train=7,
                                             delta_delay=7, delta_test=7)

    classifier = sklearn.ensemble.RandomForestClassifier(random_state=0, n_jobs=-1)

    model_and_predictions_dictionary = fit_model_and_get_predictions(classifier, train_df, test_df, input_features,
                                                                     output_feature, scale=False)
    # Save the model as a pickle in a file
    joblib.dump(model_and_predictions_dictionary['classifier'], 'trained_model.pkl')

    test_df['pred'] = model_and_predictions_dictionary['classifier'].predict(test_df[input_features])
    print(test_df[test_df['pred'] == 1])
