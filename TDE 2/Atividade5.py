# Valor médio das transações por categoria, ano e tipo de unidade considerando
# somente as transações do tipo exportação realizadas no Brasil.

from mrjob.job import MRJob

class AverageTransactionValue(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Skip invalid lines
        country = fields[0]
        year = fields[1]
        flow = fields[4]
        trade_usd = fields[5]
        quantity_name = fields[7]
        category = fields[9]
        if country == 'Brazil' and flow == 'Export':
            try:
                trade_usd = float(trade_usd)
                key = (category, year, quantity_name)
                value = (trade_usd, 1)
                yield key, value
            except ValueError:
                pass  # Skip lines with invalid trade_usd

    def combiner(self, key, values):
        total_trade_usd = 0.0
        total_count = 0
        for trade_usd, count in values:
            total_trade_usd += trade_usd
            total_count += count
        yield key, (total_trade_usd, total_count)

    def reducer(self, key, values):
        total_trade_usd = 0.0
        total_count = 0
        for trade_usd, count in values:
            total_trade_usd += trade_usd
            total_count += count
        if total_count > 0:
            average_trade_usd = total_trade_usd / total_count
        else:
            average_trade_usd = 0.0
        yield key, average_trade_usd

if __name__ == '__main__':
    AverageTransactionValue.run()

