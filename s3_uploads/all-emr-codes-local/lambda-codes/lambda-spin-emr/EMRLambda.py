from __future__ import print_function
import boto3
import os, sys
import logging as log
from config import emr_properties
from datetime import datetime
import tarfile
import logging
import time
import emr_create


from datetime import datetime, time


import datetime as dt
hour=dt.datetime.now().hour
min=dt.datetime.now().minute
min=int(min)/60
time_int=int(hour) + min
est_time=time_int-4
client = boto3.client('emr')


def lambda_handler(event, context):
    if est_time < 13:
        emr_create.run_emr_creation('SPOT')
    else:
        emr_create.run_emr_creation('ON_DEMAND')