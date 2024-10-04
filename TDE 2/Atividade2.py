#Número de transações por ano.

from mrjob.job import MRJob

class TransactionsPerYear(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return
        fields = line.split(';')
        if len(fields) != 10:
            return
        year = fields[1]
        yield year, 1

    def combiner(self, year, counts):
        yield year, sum(counts)

    def reducer(self, year, counts):
        total = sum(counts)
        yield year, total

if __name__ == '__main__':
    TransactionsPerYear.run()

