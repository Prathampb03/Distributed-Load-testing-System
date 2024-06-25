#!usr/bin/env python3

from kafka  import  KafkaProducer,KafkaConsumer
import json
import sys
from tokensGen import generate_testID
import statistics

KAFKA_IP = sys.argv[1]
ORCH_IP = sys.argv[2]

kp = KafkaProducer(bootstrap_servers = KAFKA_IP)

def sendFunc(channel,dataDictionary):
    kp.send(channel,json.dumps(dataDictionary).encode("utf-8"))
    kp.flush()



def triggerHandler(v):
    print("triggerHandler called with ",v)
    sendFunc("trigger", 
{
  "test_id": TEST_ID,
  "trigger": v,
})

def receiveFunc(msg):
    return json.loads(msg.value.decode("utf-8"))




globalMetrics={}

kc= KafkaConsumer( bootstrap_servers = KAFKA_IP  )
kc.subscribe([ "metrics","heartbeat","register"])


def listener():
    
    for msg in kc:
        print("msg recieved",msg.topic,msg.value)
        
        if msg.topic == "heartbeat":
            temp  = receiveFunc(msg)
            print("heartbeat received!"+ temp["node_id"])
        
        elif msg.topic == "metrics":
            temp  = receiveFunc(msg)
            print("metrics received!")
            # TODO process to global metrics
            if temp["node_id"] not in globalMetrics:
                globalMetrics[temp["node_id"]]=[]
            globalMetrics[temp["node_id"]].extend(temp["val"])
            for k in globalMetrics:
                print(k , len(globalMetrics[k]))

import threading
    
listenerThread = threading.Thread(target=listener)
listenerThread.start()

if __name__ == "__main__":
    
    TEST_TYPE = input().upper()
    TEST_ID = generate_testID()
    
    if TEST_TYPE == "TSUNAMI":
        print("not yet supported!")
        exit()
    
    elif TEST_TYPE == "AVALANCHE": 
        print("enter desired max throughput requests per second per driver node")
        tput = int(input())
        sendFunc("test_config", 
        {
        "test_id": TEST_ID,
        "test_type": TEST_TYPE,
        "throughput" : tput,# 100 requests per second?
        "test_message_delay": 0, #TODO no delay for now ?
        })
        
        triggerHandler("YES")
    else:
        print("invalid type!!")
 
        #exit()
 
    while True:
        

        qt = input()
        if qt.upper()=="EXIT":
            triggerHandler("NO")
            break
 
    listenerThread.join()
           
        
    kp.flush()
    kp.close()
