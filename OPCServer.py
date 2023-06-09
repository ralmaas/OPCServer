from opcua import ua, Server
from opcua.common.node import Node
from random import randint
import datetime
import time
import random
from paho.mqtt import client as mqtt_client
# Need json as the mqtt-data is in json-format
import json
# Need to make structure
from dataclasses import dataclass
from pprint import pprint

# ========================================
# Configuration section
# ========================================

# OPC UA
# Configure local IP and port to be used
# This is the IP address clients use for connection to THIS server
url = "opc.tcp://192.168.2.68:4840"

# MQTT
broker = "192.168.2.200"
port = 1883
elwiz_topic = "meter/#"
pub_topic = "opc"

# other
# Set DEBUG to True or False
DEBUG = False
VERSION = "1.8"
#
# Set value to "true" in order to multiply reading with column 2 from meter.txt
# Set to "false" to fetch just the raw data received
#
MULTIPLY = True

# ========================================
# Code section
# ========================================
@dataclass
class OPC_Class:
    meterID: Node
    power: Node
    powerReactive: Node
    powerProduction: Node
    powerProductionReactive: Node
    voltagePhase1: Node
    voltagePhase2: Node
    voltagePhase3: Node
    currentL1: Node
    currentL2: Node
    currentL3: Node
    lastMeterConsumption: Node
    lastMeterConsumptionReactive: Node
    lastMeterProduction: Node
    lastMeterProductionReactive: Node
    meterDate: Node


# OPC UA
server = Server()
client_id = f'python-mqtt-{random.randint(0, 1000)}'
server.set_endpoint(url)


# Define the global variables
meterTable = {}
meterTableFactor = {}

def publish(topic, value):
     global client
     msg = f"{value}"
     if topic == 0:
        topic2 = pub_topic + "/status"
     if topic == 1:
        topic2 = pub_topic + "/error"
     result = client.publish(topic2, msg)


def subscribe(client: mqtt_client, topic):
    client.subscribe(topic)
    client.on_message = on_message

def readMeter():
  with open('meter.txt', 'r') as f:
    for line in f:
      k = line.split()
      if k[0] != "#":
        meterTableFactor[k[0]] = k[1]
        meterTable[k[0]] = k[1]

def getMeter(key):
  try:
    meterFactorTable[key]
    return meterTable[key]
  except:
    return 0


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def on_message(client, userdata, message):
    global meterTable
    global MULTIPLY

    # print("message received " ,str(message.payload.decode("utf-8")))
    json_data = str(message.payload.decode("utf-8"))
    if DEBUG:
        print("JSON DATA:")
        pprint(json_data)
    try:
        if DEBUG:
            print("Convert to dict")
            print(json_data)
        data = json.loads(json_data)
    except ValueError:
        if DEBUG:
            print("Decoding Failed")
        return
    
    byte_ = bytes(json_data, "utf-8")
    if DEBUG:
        print("Length of object is " + str(len(byte_)))
        print("Dump of byte_:")
        pprint(byte_)
    data_size = len(byte_)
    if DEBUG:
        print("data object:")
        pprint(data)
        print("data length is: " + str(len(data)))

    if b'lastMeterConsumption' in byte_:
        #
        #   I HAVE A LIST 3
        #
        if DEBUG:
            print("I have a List 3")
            pprint(data)
        # Handle List 3 data
        # Find the correct memory space
        indx = str(data['data']['meterID'])
        try:
            opc_pointer = meterTable[indx]
        except:
            print("Error: Meter " + str(indx) + " is not found in file meter.txt")
            publish(1, "Error: Meter " + str(indx) + " is not found in file meter.txt")
            return
        
        mpy_factor = float(meterTableFactor[data['data']['meterID']])
        # Some of the variables should be multiplied by mpyFactor
        opc_pointer.meterID.set_value(data['data']['meterID'])

        opc_pointer.voltagePhase1.set_value(data['data']['voltagePhase1'])
        opc_pointer.voltagePhase2.set_value(data['data']['voltagePhase2'])
        opc_pointer.voltagePhase3.set_value(data['data']['voltagePhase3'])
        opc_pointer.meterDate.set_value(data['data']['meterDate'])

        if (MULTIPLY == False):
            mpy_factor = 1.0
        opc_pointer.power.set_value(mpy_factor*data['data']['power'])
        opc_pointer.powerReactive.set_value(mpy_factor*data['data']['powerReactive'])
        opc_pointer.powerProduction.set_value(mpy_factor*data['data']['powerProduction'])
        opc_pointer.powerProductionReactive.set_value(mpy_factor*data['data']['powerProductionReactive'])
        opc_pointer.currentL1.set_value(mpy_factor*data['data']['currentL1'])
        try:
            opc_pointer.currentL2.set_value(mpy_factor*data['data']['currentL2']) 
        except:
            opc_pointer.currentL2.set_value(0.0)
        opc_pointer.currentL3.set_value(mpy_factor*data['data']['currentL3'])

        #  Next variables are just in L3
        opc_pointer.lastMeterConsumption.set_value(mpy_factor*data['data']['lastMeterConsumption'])
        opc_pointer.lastMeterConsumptionReactive.set_value(mpy_factor*data['data']['lastMeterConsumptionReactive'])
        opc_pointer.lastMeterProduction.set_value(mpy_factor*data['data']['lastMeterProduction'])
        opc_pointer.lastMeterProductionReactive.set_value(mpy_factor*data['data']['lastMeterProductionReactive'])
 
        if DEBUG:
            print("Reached end of List 3")
        return

    if b'voltagePhase1' in byte_:
        if DEBUG:
            pprint(data)
        #
        #   I HAVE A LIST 2
        #
        # Handle List 2
        # Find the correct memory space
        indx = str(data['data']['meterID'])
        try:
            opc_pointer = meterTable[indx]
        except:
            print("Error: Meter " + str(indx) + " is not found in file meter.txt")
            publish(1, "Error: Meter " + str(indx) + " is not found in file meter.txt")
            return

        mpy_factor = float(meterTableFactor[data['data']['meterID']])
        opc_pointer.meterID.set_value(data['data']['meterID'])
        opc_pointer.voltagePhase1.set_value(data['data']['voltagePhase1'])
        opc_pointer.voltagePhase2.set_value(data['data']['voltagePhase2'])
        opc_pointer.voltagePhase3.set_value(data['data']['voltagePhase3'])

        if (MULTIPLY == False):
            mpy_factor = 1.0
        opc_pointer.power.set_value(mpy_factor*data['data']['power'])
        opc_pointer.powerReactive.set_value(mpy_factor*data['data']['powerReactive'])
        opc_pointer.powerProduction.set_value(mpy_factor*data['data']['powerProduction'])
        opc_pointer.powerProductionReactive.set_value(mpy_factor*data['data']['powerProductionReactive'])
        opc_pointer.currentL1.set_value(mpy_factor*data['data']['currentL1'])
        try:
            opc_pointer.currentL2.set_value(mpy_factor*data['data']['currentL2']) 
        except:
            opc_pointer.currentL2.set_value(0.0)
        opc_pointer.currentL3.set_value(mpy_factor*data['data']['currentL3'])
        if DEBUG: 
            print("Reached end of List 2")
        return

    # Handle other topics....
    if DEBUG:
        print("Either a bug, List1 or status - not handled")
    return

#################################################################
# main()
#################################################################
# Get meterID-file
readMeter()

name = "OPCUA_SIMULATION_SERVER"
addSpace = server.register_namespace(name)
print("AddSpace is a: ", type(addSpace))
node = server.get_objects_node()
print("node is a: ", type(node))
ServerInfo = node.add_object(addSpace, "OPCUA Simulatom Server")

print("Loop through table and add class elements:")
for indx in meterTable:
    Param = node.add_object(addSpace, str(indx))
    meterTable[indx] = OPC_Class(
        Param.add_variable(addSpace, "meterID", ""),
        Param.add_variable(addSpace, "power", 0),
        Param.add_variable(addSpace, "powerReactive", 0),
        Param.add_variable(addSpace, "powerProduction", 0),
        Param.add_variable(addSpace, "powerProductionReactive", 0),
        Param.add_variable(addSpace, "voltagePhase1", 0),
        Param.add_variable(addSpace, "voltagePhase2", 0),
        Param.add_variable(addSpace, "voltagePhase3", 0),
        Param.add_variable(addSpace, "currentL1", 0),
        Param.add_variable(addSpace, "currentL2", 0),
        Param.add_variable(addSpace, "currentL3", 0),
        Param.add_variable(addSpace, "lastMeterConsumption", 0),
        Param.add_variable(addSpace, "lastMeterConsumptionReactive", 0),
        Param.add_variable(addSpace, "lastMeterProduction", 0),
        Param.add_variable(addSpace, "lastMeterProductionReactive", 0),
        Param.add_variable(addSpace, "meterDate", 0)
    )
    meterTable[indx].meterID.set_writable()
    meterTable[indx].power.set_writable()
    meterTable[indx].powerReactive.set_writable()
    meterTable[indx].powerProduction.set_writable()
    meterTable[indx].powerProductionReactive.set_writable()
    meterTable[indx].voltagePhase1.set_writable()
    meterTable[indx].voltagePhase2.set_writable()
    meterTable[indx].voltagePhase3.set_writable()
    meterTable[indx].currentL1.set_writable()
    meterTable[indx].currentL2.set_writable()
    meterTable[indx].currentL3.set_writable()
    meterTable[indx].lastMeterConsumption.set_writable()
    meterTable[indx].lastMeterConsumptionReactive.set_writable()
    meterTable[indx].lastMeterProduction.set_writable()
    meterTable[indx].lastMeterProductionReactive.set_writable()
    meterTable[indx].meterDate.set_writable()
    
# Start OPC UA
server.start()

# Start MQTT
client = connect_mqtt()
subscribe(client,elwiz_topic)
client.loop_start()
publish(0, "OPC UA Serve; Version: " + str(VERSION))
print("OPC UA Server is running")

if DEBUG:
    pprint(meterTableFactor)
    pprint(meterTable)
print("Server started at []".format(url))
publish(0, "Server started at []".format(url))

while True:
    # Just loop and copy MQTT-received data to OPC Address-space
    # meterID.set_value(_meterID)
    time.sleep(2)

