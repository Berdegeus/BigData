from mrjob.job import MRJob

class TransactionsPerFlowYear(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip the header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Ignore invalid lines
        year = fields[1]
        flow = fields[4]
        yield (flow, year), 1

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        total = sum(counts)
        flow, year = key
        yield f'{flow},{year}', str(total)

if __name__ == '__main__':
    TransactionsPerFlowYear.run()
