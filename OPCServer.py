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
# 2023-03-23/ralm
# V1.8
# Fixed error if meterID not in meter-table
#
# V1.7
# Rewritten some of the "flow"-logic
#
# 2023-03-21/ralm
# V1.6
# New names from ElWiz
#
# 2022-01-07/ralm
# V1.5:
# Added voltageL1, voltageL2 and voltageL3
#
# 2022-01-06/ralm
# V1.4:
# Multiplyfactor implemented
#
# 2021-10-26/ralm
# V1.3:
#	Reading meterID using as array

#######################################################################################
VERSION = "1.8"
# OPC UA
# Configure local IP and port to be used
# This is the IP address clients use for connection to THIS server
url = "opc.tcp://192.168.2.68:4840"
# MQTT
broker = "192.168.2.200"
port = 1883
elwiz_topic = "meter/#"
pub_topic = "opc"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
#
# Set value to "true" in order to multiply reading with column 2 from meter.txt
# Set to "false" to fetch just the raw data received - ignoring the factor in meter.txt
MULTIPLY = True
#######################################################################################

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


def readMeter():
  with open('meter.txt', 'r') as f:
    for line in f:
      print(line)
      k = line.split()
      print(k[0])
      print(k[1])
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
    print("JSON DATA:")
    pprint(json_data)
    try:
        print("Convert to dict")
        print(json_data)
        data = json.loads(json_data)
    except ValueError:
        print("Decoding Failed")
        return
    
    byte_ = bytes(json_data, "utf-8")
    print("Length of object is " + str(len(byte_)))
    print("Dump of byte_:")
    pprint(byte_)
    data_size = len(byte_)
    print("data object:")
    pprint(data)
    print("data length is: " + str(len(data)))

    # print("Test for List 3")
    if b'lastMeterConsumption' in byte_:
        #
        #   I HAVE A LIST 3
        #
        print("I have a List 3")
        pprint(data)
        # Handle List 3 data
        # Find the correct memory space
        indx = str(data['data']['meterID'])
        #
        # Upps: What do I do if meter NOT in table ??????
        #
        try:
            opc_pointer = meterTable[indx]
        except:
            print("Error - MeterID " + str(data['data']['meterID']) + " is not in the meter-table!")
            publish(1, "Error - MeterID " + str(data['data']['meterID']) + " is not in the meter-table!")
            # MeterID not in table
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
 
        print("Reached end of List 3")
        return

    if b'voltagePhase1' in byte_:
        pprint(data)
        #
        #   I HAVE A LIST 2
        #
        # Handle List 2
        # Find the correct memory space
        #printf("Value of indx is %s", indx)
        try:
            opc_pointer = meterTable[indx]
        except:
            print("Error - MeterID " + str(data['data']['meterID']) + " is not in the meter-table!")
            publish(1, "Error - MeterID " + str(data['data']['meterID']) + " is not in the meter-table!")
            # MeterID not in table
            return
        #
        #   One more Uppps - what if not in table
        # 
        # print("==> Check if meter in table: " + str(indx) + " - " + str(opc_pointer))
        # print("==> Check if meter in table: " + indx + " ====>>> " + str(opc_pointer))

        mpy_factor = float(meterTableFactor[data['data']['meterID']])
        opc_pointer.meterID.set_value(data['data']['meterID'])
        opc_pointer.voltagePhase1.set_value(data['data']['voltagePhase1'])
        opc_pointer.voltagePhase2.set_value(data['data']['voltagePhase2'])
        opc_pointer.voltagePhase3.set_value(data['data']['voltagePhase3'])
        #printf("Value of V1 is: %f", data['data']['voltagePhase1'])

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
        print("Reached end of List 2")
        return

    # Handle List 1
    print("Either a bug, List1 or status - not handled")
    return

def subscribe(client: mqtt_client, topic):
    client.subscribe(topic)
    client.on_message = on_message



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
    # print(indx)
    # print("\t" + str(meterTable[indx]))
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
# publish(0, "OPC UA Server is running")
publish(0, "OPC UA Server version: " + str(VERSION))
print("OPC UA Server is running")

pprint(meterTableFactor)
pprint(meterTable)
print("Server started at []".format(url))

while True:
    # Just loop and copy MQTT-received data to OPC Address-space
    # meterID.set_value(_meterID)
    time.sleep(2)

