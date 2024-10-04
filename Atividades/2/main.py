from mrjob.job import MRJob
import re
import json

class WordCount(MRJob):
  def mapper(self, _, value):
    review = json.loads(value)
    overallList = review['overall']
    for len(overallList) in overallList:
      yield overallList, 1

def reducer(self, key, values):
  for value in values:
    if (value) >= 2.5:
      yield key, (values)
    elif (value) < 2.5:
      yield key, (values)

if __name__ == '__main__':
  WordCount.run()