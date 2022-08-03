from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import argparse
import json

host = "a2xg0m2ynir6td-ats.iot.us-east-2.amazonaws.com"
rootCAPath = "D:\\AWS_test\\certificates\\AmazonRootCA1.pem"
certificatePath = "D:\\AWS_test\\certificates\\certificate.pem.crt"
privateKeyPath = "D:\\AWS_test\\certificates\\private.pem.key"
port = 8883
clientId = "PriyeshSubTesting"

# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n")

# Init SubscribeTestClient
SubscribeTestClient = None
SubscribeTestClient = AWSIoTMQTTClient(clientId)
SubscribeTestClient.configureEndpoint(host, port)
SubscribeTestClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# SubscribeTestClient connection configuration
SubscribeTestClient.configureAutoReconnectBackoffTime(1, 32, 20)
SubscribeTestClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
SubscribeTestClient.configureDrainingFrequency(2)  # Draining: 2 Hz
SubscribeTestClient.configureConnectDisconnectTimeout(10)  # 10 sec
SubscribeTestClient.configureMQTTOperationTimeout(5)  # 5 sec

SubscribeTestClient.connect()
SubscribeTestClient.subscribe("test/subtesting", 0, customCallback)

while(1):
    time.sleep(1)

#SubscribeTestClient.unsubscribe("test/subtesting")

