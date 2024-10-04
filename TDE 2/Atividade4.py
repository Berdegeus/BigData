#Valor médio das transações por ano.

from mrjob.job import MRJob

class AverageTradeUSDPerYear(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip the header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Ignore invalid lines
        year = fields[1]
        trade_usd = fields[5]
        try:
            trade_usd = float(trade_usd)
            yield year, (trade_usd, 1)
        except ValueError:
            pass  # Skip lines with invalid trade_usd

    def combiner(self, year, values):
        total_trade_usd = 0.0
        total_count = 0
        for trade_usd, count in values:
            total_trade_usd += trade_usd
            total_count += count
        yield year, (total_trade_usd, total_count)

    def reducer(self, year, values):
        total_trade_usd = 0.0
        total_count = 0
        for trade_usd, count in values:
            total_trade_usd += trade_usd
            total_count += count
        if total_count > 0:
            average = total_trade_usd / total_count
            yield year, average
        else:
            yield year, 0

if __name__ == '__main__':
    AverageTradeUSDPerYear.run()
