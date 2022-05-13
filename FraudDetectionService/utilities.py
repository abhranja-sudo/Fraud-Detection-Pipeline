# General
import os
import pandas as pd
import numpy as np
import math
import sys
import time
import pickle
import json
import datetime
import random
#import sklearn
import pyspark.pandas as pd
from pyspark.ml.feature import VectorAssembler

from os import path
from os import listdir
import json


import sklearn
from sklearn import *

# def read_from_files(DIR_INPUT, BEGIN_DATE, END_DATE):
#     files = [os.path.join(DIR_INPUT, f) for f in os.listdir(DIR_INPUT) if
#              f >= BEGIN_DATE + '.pkl' and f <= END_DATE + '.pkl']
#
#     frames = []
#     for f in files:
#         df = pd.read_pickle(f)
#         frames.append(df)
#         del df
#     df_final = pd.concat(frames)
#
#     df_final = df_final.sort_values('TRANSACTION_ID')
#     df_final.reset_index(drop=True, inplace=True)
#     #  Note: -1 are missing values for real world data
#     df_final = df_final.replace([-1], 0)
#
#     return df_final

def read_from_files(DIR_INPUT, BEGIN_DATE, END_DATE):
    files = [os.path.join(DIR_INPUT, f) for f in os.listdir(DIR_INPUT) if
             f >= BEGIN_DATE + '.pkl' and f <= END_DATE + '.pkl']

    # new_files = new_file_list()
    #
    # files = files + new_files

    print('FILES',files)
    frames = []
    for f in files:
        df = pd.read_csv(f,sep='\t')
        frames.append(df)
        del df
    df_final = pd.concat(frames)

    df_final = df_final.sort_values('TRANSACTION_ID')
    df_final.reset_index(drop=True)
    #  Note: -1 are missing values for real world data
    df_final = df_final.replace([-1], 0)

    return df_final



def scaleData(train, test, features):
    assembler = VectorAssembler(inputCols=features, outputCol="feature")
    train_df = assembler.transform(train.to_spark())
    test_df = assembler.transform(test.to_spark())
    # print('This is the testing set', test_df.toPandas())
    return (train_df, test_df)

TRANSACTIONS_PATH = os.getcwd() + '/transactions'

def new_file_list() :
    file_list = []
    for files in listdir(TRANSACTIONS_PATH) :
        file = files.split('.')[0]
        date_time = datetime.datetime.strptime(file, "%Y-%m-%d")
        no_of_days = (date_time.now().date() - date_time.date()).days

        if no_of_days == 0 and files.find('.csv') != -1:
            file_list.append('transactions/'+files)

    return file_list

