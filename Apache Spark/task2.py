import sys
import csv
from pyspark import SparkContext, SparkConf

# Get the borough info


def getBorough(lines):
    requiredIndexes = [1, 4, 6, 8, 10]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[4]) & any(c.isalpha() for c in lines[6])):
        boroughKilled = 0
    else:
        # Extremely messy way of getting total killed
        boroughKilled = int(float(lines[4][2])) + int(float(lines[6][2])) + \
            int(float(lines[8][2])) + int(float(lines[10][2]))

    # Build up an array of the borough, and total hurt people
    ret.append(lines[1])
    ret.append(boroughKilled)
    return ret

# Get the zip code info


def getZipCode(lines):
    requiredIndexes = [2, 4, 6, 8, 10]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[4]) & any(c.isalpha() for c in lines[6])):
        boroughKilled = 0
    else:
        # Extremely messy way of getting total killed
        boroughKilled = int(float(lines[4][2])) + int(float(lines[6][2])) + \
            int(float(lines[8][2])) + int(float(lines[10][2]))

    # Build up an array of the zip code, and total hurt people
    ret.append(lines[2])
    ret.append(boroughKilled)
    return ret

# Get the total amount of injured pedestrians


def getInjured(lines):
    requiredIndexes = [0, 3, 5]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[3]) & any(c.isalpha() for c in lines[5])):
        totalHurt = 0
    else:
        # Extremely messy way of getting total ped/persons injured
        totalHurt = int(float(lines[3][2])) + int(float(lines[5][2]))

    # Build up an array of the date, and total hurt people
    ret.append(lines[0])
    ret.append(totalHurt)
    return ret

# Get the total amount of killed pedestrians


def getKilled(lines):
    requiredIndexes = [0, 4, 6]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[4]) & any(c.isalpha() for c in lines[6])):
        totalHurt = 0
    else:
        # Extremely messy way of getting total ped/persons killed
        totalHurt = int(float(lines[4][2])) + int(float(lines[6][2]))

    # Build up an array of the date, and total hurt people
    ret.append(lines[0])
    ret.append(totalHurt)
    return ret


# Get the total amount of hurt cyclist
def getCyclist(lines):
    requiredIndexes = [0, 7, 8]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[7]) & any(c.isalpha() for c in lines[8])):
        totalHurt = 0
    else:
        # Extremely messy way of getting total hurt
        totalHurt = int(float(lines[7][2])) + int(float(lines[8][2]))

    # Build up an array of the date, and total hurt people
    ret.append(lines[0])
    ret.append(totalHurt)
    return ret

# Get the total amount of hurt motorist


def getMotorist(lines):
    requiredIndexes = [0, 9, 10]
    ret = []
    # In case there is a header still
    if(any(c.isalpha() for c in lines[9]) & any(c.isalpha() for c in lines[10])):
        totalHurt = 0
    else:
        # Extremely messy way of getting total hurt
        totalHurt = int(float(lines[9][2])) + int(float(lines[10][2]))

    # Build up an array of the date, and total hurt people
    ret.append(lines[0])
    ret.append(totalHurt)
    return ret

# Return back the


def getYear(line):
    date = line[0][-5:]
    return date[:-1]


if __name__ == "__main__":
    conf = SparkConf().setAppName("Proj 2")
    sc = SparkContext(conf=conf)

    file = "task1NypdNoHeaderOutput"
    # task1NypdOutput
    # task1output
    # task1NypdNoHeaderOutput

    # Split the cleaned data by ,
    tokenized = sc.textFile(file).map(lambda line: line.split(","))

    # Pull out the desired data column for date/borough/zip/typeCode
    dates = tokenized.map(lambda line: (line[0]))
    boroughs = tokenized.map(lambda line: (line[1]))
    zips = tokenized.map(lambda line: (line[2]))
    typeCode1 = tokenized.map(lambda line: (line[11]))

    # The reduce logic is here. First map the required data to get the total of whatever we are looking for, then reduce, and find the max value of that reduction.
    dateTotal = dates.map(lambda date: (date, 1)).reduceByKey(
        lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    boroughTotal = tokenized.map(lambda line: getBorough(line)).map(lambda line: (
        line[0], line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    zipsTotal = tokenized.map(lambda line: getZipCode(line)).map(lambda line: (
        line[0], line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    typeCode1Total = typeCode1.map(lambda typeCode: (typeCode, 1)).reduceByKey(
        lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    peopleInjuredTotal = tokenized.map(lambda line: getInjured(line)).map(lambda line: (
        getYear(line), line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    peopleKilledTotal = tokenized.map(lambda line: getKilled(line)).map(lambda line: (
        getYear(line), line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    cyclistTotal = tokenized.map(lambda line: getCyclist(line)).map(lambda line: (
        getYear(line), line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])
    motoristKilledTotal = tokenized.map(lambda line: getMotorist(line)).map(lambda line: (
        getYear(line), line[1])).reduceByKey(lambda v1, v2: v1 + v2).max(key=lambda x: x[1])

    print "Date with amount of accidents: " + dateTotal[0] + " on " + str(dateTotal[1])
    print "Borough with most accident fatality: " + boroughTotal[0] + " with " + str(boroughTotal[1]) + " deaths"
    print "Zip code with mose accident fatality: " + str(zipsTotal[0]) + " with " + str(zipsTotal[1]) + " deaths"
    print "Vehicle type with most accidents: " + typeCode1Total[0] + " with " + str(typeCode1Total[1])
    print "Year with most persons/peds injured: " + peopleInjuredTotal[0] + " with " + str(peopleInjuredTotal[1]) + " injuries"
    print "Year with most persons/peds killed: " + peopleKilledTotal[0] + " with " + str(peopleKilledTotal[1]) + " deaths"
    print "Year with most cyclists killed/injured: " + cyclistTotal[0] + " with " + str(cyclistTotal[1]) + " killed/injured"
    print "Year with most motorists killed/injured: " + motoristKilledTotal[0] + " with " + str(motoristKilledTotal[1]) + " killed/injured"
