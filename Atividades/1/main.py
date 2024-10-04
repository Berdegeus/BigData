import re
from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, _, value):
        WordLen = 0
        words = re.findall(r'\b[a-z]+\b', value.lower())
        for word in words:
            yield word, 1
            WordLen += len(word)

    def reducer(self, key, values):
        yield key, sum(values)

class WordLen(WordCount):
    def mapper(self, _, value):
        words = re.findall(r'\b[a-z]+\b', value.lower())

        for word in words:
            yield word, len(word)

if __name__ == '__main__':
    WordLen.run()
