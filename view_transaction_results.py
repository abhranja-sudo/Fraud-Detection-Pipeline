from os import path
from os import listdir
import os
from datetime import datetime
import json
import pandas as pd

TRANSACTIONS_PATH = os.getcwd() + '/transactions'

def merge_valid_transactions_to_dataset() :
    transaction_list = []
    for files in listdir(TRANSACTIONS_PATH) :
        file = files.split('.')[0]
        date_time = datetime.strptime(file, "%m-%d-%YT%H:%M:%S")
        no_of_days = (date_time.now().date() - date_time.date()).days

        if no_of_days == 0 :
            f = open('transactions/'+ file + '.json','r+').read()

            if len(f) > 1 :
                data = json.loads(f)
                transaction_list += data

            f = open('transactions/'+str(date_time.now().date()) + '.csv', 'w+')
            print(transaction_list)
            json.dump(transaction_list, f)
            transaction_list = []


merge_valid_transactions_to_dataset()