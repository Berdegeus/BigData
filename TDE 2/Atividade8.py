# Descrição e valor por quilograma da commodity mais lucrativa por peso
# comercializada em 2015, por tipo de fluxo. Considerar somente transações com Number of
# items, em que é possível calcular o peso por unidade e o valor por quilo.

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostProfitableCommodityPerWeight(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        if line.startswith('country_or_area;'):
            return  # Skip header
        fields = line.split(';')
        if len(fields) != 10:
            return  # Skip invalid lines
        year = fields[1]
        flow = fields[4]
        commodity = fields[3]
        trade_usd = fields[5]
        weight_kg = fields[6]
        quantity_name = fields[7]
        quantity = fields[8]
        if (year == '2015' and quantity_name == 'Number of items' and
            weight_kg and quantity and trade_usd):
            try:
                trade_usd = float(trade_usd)
                weight_kg = float(weight_kg)
                quantity = float(quantity)
                if weight_kg > 0 and quantity > 0:
                    value_per_kg = trade_usd / weight_kg
                    key = (flow, commodity)
                    yield key, value_per_kg
            except ValueError:
                pass  # Skip lines with invalid numerical values

    def combiner(self, key, values):
        # Keep the maximum value_per_kg for each key (flow, commodity)
        max_value_per_kg = max(values)
        yield key, max_value_per_kg

    def reducer_find_max_per_commodity(self, key, values):
        # For each key (flow, commodity), keep the maximum value_per_kg
        max_value_per_kg = max(values)
        flow, commodity = key
        yield flow, (max_value_per_kg, commodity)

    def reducer_find_most_profitable(self, flow, value_commodity_pairs):
        # For each flow, find the commodity with the highest value_per_kg
        max_value_per_kg = 0.0
        max_commodity = ''
        for value_per_kg, commodity in value_commodity_pairs:
            if value_per_kg > max_value_per_kg:
                max_value_per_kg = value_per_kg
                max_commodity = commodity
        # Output: flow, commodity, value_per_kg
        yield flow, (max_commodity, max_value_per_kg)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_find_max_per_commodity),
            MRStep(reducer=self.reducer_find_most_profitable)
        ]

if __name__ == '__main__':
    MostProfitableCommodityPerWeight.run()
