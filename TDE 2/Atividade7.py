# Descrição e quantidade total de itens da commodity mais comercializada em 2014
# pela China, por tipo de fluxo. Considerar somente transações com Number of items.

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostTradedCommodity(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Skip invalid lines
        country = fields[0]
        year = fields[1]
        commodity = fields[3]
        flow = fields[4]
        quantity_name = fields[7]
        quantity = fields[8]
        if country == 'China' and year == '2014' and quantity_name == 'Number of items':
            try:
                quantity = float(quantity)
                key = (flow, commodity)
                yield key, quantity
            except ValueError:
                pass  # Skip lines with invalid quantity

    def combiner(self, key, quantities):
        yield key, sum(quantities)

    def reducer_sum_quantities(self, key, quantities):
        total_quantity = sum(quantities)
        flow, commodity = key
        yield flow, (total_quantity, commodity)

    def reducer_find_max(self, flow, quantity_commodity_pairs):
        max_quantity = 0
        max_commodity = ''
        for total_quantity, commodity in quantity_commodity_pairs:
            if total_quantity > max_quantity:
                max_quantity = total_quantity
                max_commodity = commodity
        # Output format: flow,commodity,total_quantity
        yield flow, f'{max_commodity},{max_quantity}'

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_sum_quantities),
            MRStep(reducer=self.reducer_find_max)
        ]

if __name__ == '__main__':
    MostTradedCommodity.run()
