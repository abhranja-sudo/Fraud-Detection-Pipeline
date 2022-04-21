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
import sklearn
from sklearn import *

def read_from_files(DIR_INPUT, BEGIN_DATE, END_DATE):
    files = [os.path.join(DIR_INPUT, f) for f in os.listdir(DIR_INPUT) if
             f >= BEGIN_DATE + '.pkl' and f <= END_DATE + '.pkl']

    frames = []
    for f in files:
        df = pd.read_pickle(f)
        frames.append(df)
        del df
    df_final = pd.concat(frames)

    df_final = df_final.sort_values('TRANSACTION_ID')
    df_final.reset_index(drop=True, inplace=True)
    #  Note: -1 are missing values for real world data
    df_final = df_final.replace([-1], 0)

    return df_final


def scaleData(train, test, features):
    scaler = sklearn.preprocessing.StandardScaler()
    scaler.fit(train[features])
    train[features] = scaler.transform(train[features])
    test[features] = scaler.transform(test[features])

    return (train, test)

