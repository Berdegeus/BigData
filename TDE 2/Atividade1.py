#Número de transações envolvendo Australia, Brasil e China.

from mrjob.job import MRJob

class CountryTransactionCounter(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip header
        fields = line.split(';')
        if len(fields) != 10:
            return
        country = fields[0]
        if country in ['Australia', 'Brazil', 'China']:
            yield country, 1

    def combiner(self, country, counts):
        yield country, sum(counts)

    def reducer(self, country, counts):
        total = sum(counts)
        yield country, total

if __name__ == '__main__':
    CountryTransactionCounter.run()

