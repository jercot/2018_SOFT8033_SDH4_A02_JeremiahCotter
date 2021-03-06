# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json
from __future__ import division

def parseData(inData):
  total = 0
  neg = 0
  score = 0
  for x in inData:
    if x["evaluation"]=="Positive":
      score += x["points"]
    elif x["evaluation"]=="Negative":
      score -= x["points"]
      neg += 1
    total+=1
  return (total, neg, score, score/total)


def filter(data, percent, average):
  if 100*data[1]/data[0]<percent and data[0]>average:
    return True;
  return False;

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  inputRDD = sc.textFile(dataset_dir).map(json.loads)
  inputRDD.cache()
  mapRDD = inputRDD.groupBy(lambda x: x["cuisine"]).map(lambda x: (x[0], parseData(list(x[1]))))
  mapRDD.cache()
  average = inputRDD.count()/mapRDD.count()
  filterRDD =  mapRDD.filter(lambda x: filter(x[1], percentage_f, average))
  outputRDD = filterRDD.sortBy(lambda x: -x[1][3])
  outputRDD.saveAsTextFile(result_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)