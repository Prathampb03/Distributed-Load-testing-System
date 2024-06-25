#!usr/bin/env python3

import threading
from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import time
#from tokensGen import generate_random_token,generate_reportID
import statistics
import sys
import random

def generate_random_token():
    # Generate a random 6-digit token with numbers only
    token = ''.join(random.choices('0123456789', k=6))
    return token

def generate_testID():
    # Generate a random 10-digit token with numbers only
    token = ''.join(random.choices('0123456789', k=10))
    return token

def generate_reportID():
    # Generate a random 5-digit token with numbers only
    token = ''.join(random.choices('0123456789', k=5))
    return token

KAFKA_IP = sys.argv[1]
ORCH_IP = sys.argv[2]

kp = KafkaProducer(bootstrap_servers=KAFKA_IP)

threadLock = threading.Lock()
NODE_ID = generate_random_token()
result = []  # Define the res list
full=[]
mean=0
n=0
m2=float("inf")
m1=0
trigger_event = threading.Event()  # Event to trigger the metric function

TEST_CONFIG = None

# Add a global variable to store TEST_TYPE
TEST_TYPE = None

def sendFunc(message_type, data):

    # Check if the message type is "test_config" and set TEST_TYPE
    if message_type == "test_config":
        global TEST_TYPE
        TEST_TYPE = data.get("test_type")
        print(TEST_TYPE)



def heartBeat():
    data = {
        "node_id": NODE_ID,
        "heartbeat": "YES",
    }
    isHeartbeatActive = True
    while isHeartbeatActive:
        print("lub dub")
        sendFunc("heartbeat",data)
        time.sleep(8)

def metric():
    while True:
        if TEST_CONFIG == None:
            print("ERROR : No test config found !Exiting")
            exit()
        # Wait for the trigger event to be set before running the metric function
        global mean,m1,m2,n
        trigger_event.wait()
        while len(result)==0:
            print("empty res waiting")
            time.sleep(0.2)
            pass
        threadLock.acquire()
        res_len=len(result)
        tot_len=n+res_len
        mean = (sum(result)+mean*n)/(tot_len)
	#throughput = (res_len) / 10
        b=tot_len
        m1=max(m1,max(result))
        m2=min(m2,min(result))
        full.extend(result)
        median = statistics.median(full)
        
        
        sendFunc("metrics",{ "node_id" :NODE_ID , "test_id" : TEST_CONFIG["test_id"], "report_id" : generate_reportID() , "l":res_len,"val":result ,  "metrics":{"mean_latency":mean,"median_latency":median,"max_latency":m1,"min_latency":m2}})
        result.clear()
        threadLock.release()
        print(mean,median)
        
        # Clear the event to ensure it runs only once
        trigger_event.clear()
        time.sleep(1)

metric_thread = threading.Thread(target=metric)

def metric_2():
    while True:
        if TEST_CONFIG == None:
            print("ERROR : No test config found !Exiting")
            exit()
        # Wait for the trigger event to be set before running the metric function
        global mean,m1,m2,n
        trigger_event.wait()
        while len(result)==0:
            print("empty res waiting")
            time.sleep(0.2)
            pass
        threadLock.acquire()
        res_len=len(result)
        tot_len=n+res_len
        mean = (sum(result)+mean*n)/(tot_len)
        b=tot_len
        m1=max(m1,max(result))
        m2=min(m2,min(result))
        full.extend(result)
        median = statistics.median(full)
        
        
        sendFunc("metrics",{ "node_id" :NODE_ID , "test_id" : TEST_CONFIG["test_id"], "report_id" : generate_reportID() , "l":res_len,"val":result ,  "metrics":{"mean_latency":mean,"median_latency":median,"max_latency":m1,"min_latency":m2}})
        result.clear()
        threadLock.release()
        print(mean,median)
        
        # Clear the event to ensure it runs only once
        trigger_event.clear()
        time.sleep(1)

def sendFunc(channel, dataDictionary):
    kp.send(channel, json.dumps(dataDictionary).encode("utf-8"))
    kp.flush()

def getReq():
    try:
        t1 = time.time()
        resp = requests.get(SERVER_URL)
        t2 = time.time()
        threadLock.acquire()
        result.append(t2 - t1)
        threadLock.release()
    except Exception as e:
        print(e)
        print("error while trying to GET server @ " + str(time.localtime(t1)))

FREQ = None

def receiveFunc(msg):
    return json.loads(msg.value.decode("utf-8"))

def send_metrics_to_server(metrics_data):
    server_url = "http://127.0.0.1:5000/metrics"
    requests.post(server_url, json=metrics_data)

def reqTh():
    global reqCount , intervalsStart , start
    start=time.time()
    reqCount=0
    intervalsStart=start
    while time.time()-start<20:
        
        getReq()
        reqCount+=1
        if reqCount==TEST_CONFIG["throughput"]:
            while time.time()-intervalsStart<=1:
                pass
            intervalsStart=time.time()
            reqCount=0
        # Set the trigger event to run the metric function
        trigger_event.set()
        # Send metrics to the server after the test is completed
    metrics_data = {
        "node_id": NODE_ID,
        "test_id": TEST_CONFIG["test_id"],
        "metrics": {"mean_latency": mean, "max_latency": m1, "min_latency": m2}
    }
    send_metrics_to_server(metrics_data)
    print("OPERATION COMPLETED!")
    kp.flush()
    kp.close()
    requestThread.join(0)
    metric_thread.join(0)
    sys.exit()

requestThread =  threading.Thread(target=reqTh)

if __name__ == "__main__":
    print(generate_random_token())
    print(generate_testID())
    
    SERVER_URL = "http://127.0.0.1:5000"
    kc = KafkaConsumer(bootstrap_servers=KAFKA_IP)
    kc.subscribe(["test_config","register","trigger"])
    sendFunc("register", {
  "node_id": NODE_ID,
  "node_IP": NODE_ID,
  "message_type": "DRIVER_NODE_REGISTER",
})

    starter = threading.Thread(target=heartBeat)
    starter.start()



    for msg in kc:
        print("msg received", msg.topic, msg.value)

        if msg.topic == "test_config":
            TEST_CONFIG = receiveFunc(msg)
            print("test_config recieved!",TEST_CONFIG)
            print(TEST_CONFIG['test_type'])
        
        
        elif msg.topic == "trigger":
            data = receiveFunc(msg)
            if data["trigger"]=="NO":
                print("exiting")
                kp.flush()
                kp.close()
                requestThread.join(0)
                metric_thread.join(0)
                sys.exit()
            
            requestThread.start()
            metric_thread.start()
            result.clear()# Clear the res list
            print("hello")
        
            
             

    kp.flush()
    kp.close()

