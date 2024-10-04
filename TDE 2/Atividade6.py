from mrjob.job import MRJob

class MinTransactionPerYearCategory(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Skip invalid lines
        year = fields[1]
        category = fields[9]
        trade_usd = fields[5]
        commodity = fields[3]
        try:
            trade_usd = float(trade_usd)
            key = (year, category)
            value = (trade_usd, commodity)
            yield key, value
        except ValueError:
            pass  # Skip lines with invalid trade_usd

    def combiner(self, key, values):
        min_trade_usd = float('inf')
        min_commodity = ''
        for trade_usd, commodity in values:
            if trade_usd < min_trade_usd:
                min_trade_usd = trade_usd
                min_commodity = commodity
        yield key, (min_trade_usd, min_commodity)

    def reducer(self, key, values):
        min_trade_usd = float('inf')
        min_commodity = ''
        for trade_usd, commodity in values:
            if trade_usd < min_trade_usd:
                min_trade_usd = trade_usd
                min_commodity = commodity
        year, category = key
        # Yield the data as key-value pairs
        yield (year, category), (min_commodity, min_trade_usd)

if __name__ == '__main__':
    MinTransactionPerYearCategory.run()
