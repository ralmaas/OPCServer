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
    #lastHourActivePower: Node


# OPC UA
server = Server()
# Configure local IP and port to be used
# This is the IP address clients use for connection to THIS server
url = "opc.tcp://192.168.2.68:4840"
server.set_endpoint(url)

# MQTT
# Note: Tibber Pulse only support anonymous or certificate-based MQTT-authentification
broker = "192.168.2.200"
port = 1883
elwiz_topic = "meter/#"
pub_topic = "opc"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

#
# Set value to "true" in order to multiply reading with column 2 from meter.txt
# Set to "false" to fetch just the raw data received
#
MULTIPLY = True

# Define the global variables
meterTable = {}
meterTableFactor = {}


def readMeter():
    global meterTable
    global meterTableFactor

    file1 = open('meter.txt', 'r')
    while True:

        # Get next line from file
        line = file1.readline()
        lineArray = line.split("\t")
        # if line is empty
        # end of file is reached
        if not line:
            break
        if (lineArray[0] != "#"):
            # Add a temporary value to the array - later to be a pointer to the OPC_Class
            meterTable[lineArray[0]] = lineArray[1]
            meterTableFactor[lineArray[0]] = int(lineArray[1])
            print("Reading Meter table %s with factor %d" % (lineArray[0],meterTableFactor[lineArray[0]] ))
    file1.close()

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
    global meterTableFactor
    global MULTIPLY

    print("message received " ,str(message.payload.decode("utf-8")))
    json_data = str(message.payload.decode("utf-8"))
    try:
        print("Convert to dict")
        print(json_data)
        data = json.loads(json_data)
    except ValueError:
        print("Decoding Failed")

    try:
        print("Test for List 3")
        print("List3 identifier: ", data['data']['cumuHourPowImpActive'])
        # Test if object exist - if not fall through to next except!
        data['data']['cumuHourPowImpActive']
        #
        #   I HAVE A LIST 3
        #
        print("I have a List 3")
        # Handle List 3 data
        # Find the correct memory space
        indx = str(data['data']['meterID'])
        opc_pointer = meterTable[indx]
        print("==> Check if meter in table: " + str(indx) + " - " + str(opc_pointer))
        mpy_factor = float(meterTableFactor[data['data']['meterID']])
        print("I have a 3-factor set at: " + str(mpy_factor))
        # Some of the variables should be multiplied by mpyFactor
        opc_pointer.meterID.set_value(data['data']['meterID'])

        opc_pointer.voltageL1.set_value(data['data']['voltagePhaseL1'])
        opc_pointer.voltageL2.set_value(data['data']['voltagePhase2'])
        opc_pointer.voltageL3.set_value(data['data']['voltagePhase3'])

        if (MULTIPLY == False):
           mpy_factor = 1.0
        opc_pointer.powImpActive.set_value(mpy_factor*data['data']['powImpActive'])
        opc_pointer.powExpActive.set_value(mpy_factor*data['data']['powExpActive'])
        opc_pointer.powImpReactive.set_value(mpy_factor*float(data['data']['powImpReactive']))
        opc_pointer.powExpReactive.set_value(mpy_factor*data['data']['powExpReactive'])
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
        # opc_pointer.lastHourActivePower.set_value(mpy_factor*data['data']['lastHourActivePower'])

        print("Reached end of List 3")
        return
    except:
        print("It was not a List 3")
        try:
            data['data']['voltagePhase1']
            #
            #   I HAVE A LIST 2
            #
            # Handle List 2
            # Find the correct memory space
            indx = str(data['data']['meterID'])
            #printf("Value of indx is %s", indx)
            opc_pointer = meterTable[indx]
            # print("==> Check if meter in table: " + str(indx) + " - " + str(opc_pointer))
            print("==> Check if meter in table: " + indx + " ====>>> " + str(opc_pointer))

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
            return
        except:
            # Handle List 1
            print("Either a bug or List 1 is not handled")
            return
    # print("End of on_message")

def subscribe(client: mqtt_client, topic):
    client.subscribe(topic)
    client.on_message = on_message

def publish(value):
     global client
     msg = f"{value}"
     topic2 = pub_topic + "/startup"
     result = client.publish(topic2, msg)


# Get meterID-file
readMeter()

print("Test lookup to meter-table")
# print(meterTable['7359992894592315'])

print("Loop through table:")
for indx in meterTable:
    print(indx)
    print("\t" + str(meterTable[indx]))

name = "OPCUA_SIMULATION_SERVER"
addSpace = server.register_namespace(name)
print("AddSpace is a: ", type(addSpace))
node = server.get_objects_node()
print("node is a: ", type(node))
ServerInfo = node.add_object(addSpace, "OPCUA Simulatom Server")

print("Loop through table and add class elements:")
for indx in meterTable:
    print(indx)
    print("\t" + str(meterTable[indx]))
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
        # Param.add_variable(addSpace, "lastHourActivePower", 0)
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
    # meterTable[indx].lastHourActivePower.set_writable()

# Start OPC UA
server.start()

# Start MQTT
client = connect_mqtt()
subscribe(client,elwiz_topic)
client.loop_start()
publish("OPC UA Server is running")
print("OPC UA Server is running")

print("Server started at []".format(url))
while True:
    # Just loop and copy MQTT-received data to OPC Address-space
    # meterID.set_value(_meterID)
    time.sleep(2)

