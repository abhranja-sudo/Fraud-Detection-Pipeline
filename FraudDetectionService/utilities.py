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


def scaleData(test, features):
    assembler = VectorAssembler(inputCols=features, outputCol="feature")
    test_df = assembler.transform(test.to_spark())
    return test_df



