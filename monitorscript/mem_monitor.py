#!/usr/bin/python3
import psutil 
import time
import os
import csv
import _thread
import datetime
def startspark(threadName):
    os.system("spark-submit --master spark://macor:7077  --class PagerankWiki C:\\Users\chris\Desktop\GRAPHX\out\\artifacts\GRAPHX_jar\GRAPHX.jar")

def monitoemem(threadName):
    with open("test.csv","w",newline='') as csvfile: 
        writer = csv.writer(csvfile)

        for x in range(100):
            time.sleep(0.1)
            writer.writerow(( datetime.datetime.now(),psutil.virtual_memory().used))

try:
    _thread.start_new_thread( startspark, ("Thread-1",))
    _thread.start_new_thread( monitoemem, ("Thread-2",))
except:
   print ("Error: unable to start thread")

while 1:
   pass