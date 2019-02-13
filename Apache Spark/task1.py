import sys
import csv
from pyspark import SparkContext, SparkConf


def removeColumns(lines):
    ## Remove the unnecessary columns besides those required for the project
    requiredIndexes = [0, 2, 3, 10, 11, 12, 13, 14, 15, 16, 17, 24]
    ret = []
    if len(lines) > 24:
        for index in requiredIndexes:
            if(lines[index]):
                ret.append(lines[index])
        return ret
    else:
        return ret


def checkValue(value):
    ## If a line doesnt have more than 11 columns, we know its an invalid row.
    if len(value) > 11:
        return value
    else:
        return ""


if __name__ == "__main__":
    ## Config
    conf = SparkConf().setAppName("Proj 2")
    sc = SparkContext(conf=conf)

    file = sys.argv[1]

    ## Use csv reader to get each line, and its column values. Then, remove rows if theyre incomplete.
    tokenized = sc.textFile(file).map(lambda line: next(
        csv.reader([line]))).filter(lambda value: checkValue(value))
    data = tokenized.map(lambda line: removeColumns(line)).filter(lambda t:checkValue(t))


    ## Save our data in a text file to be used in task2
    data.saveAsTextFile("/user/kumpaw/task1output")
    
