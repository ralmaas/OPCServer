import collections

# Define the global variables

def readMeter():
    meterTable =  {}
    file1 = open('meter.txt', 'r')
    while True:

        # Get next line from file
        line = file1.readline()
        line2 = line.split("\t")
        # if line is empty
        # end of file is reached
        if not line:
            break
        if (line2[0] != "#"):
            meterTable[len(meterTable)] = {'meter': line2[0], 'factor': line2[1]};
    file1.close()

readMeter()
