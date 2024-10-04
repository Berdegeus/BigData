#Descrição da commodity e custo da transação de menor valor por ano e categoria.

import sys

def mapper(input_data):
    mapped_data = []
    for line in input_data:
        line = line.strip()
        if line.startswith('country_or_area;'):
            continue
        fields = line.split(';')
        if len(fields) != 10:
            continue
        year = fields[1]
        category = fields[9]
        trade_usd = fields[5]
        commodity = fields[3]
        try:
            trade_usd = float(trade_usd)
            mapped_data.append(((year, category), (trade_usd, commodity)))
        except ValueError:
            continue  # Skip lines with invalid trade_usd
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for key, (trade_usd, commodity) in mapped_data:
        min_trade_usd, min_commodity = combined_data.get(key, (float('inf'), ''))
        if trade_usd < min_trade_usd:
            combined_data[key] = (trade_usd, commodity)
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for key, (trade_usd, commodity) in combined_data:
        min_trade_usd, min_commodity = reduced_data.get(key, (float('inf'), ''))
        if trade_usd < min_trade_usd:
            reduced_data[key] = (trade_usd, commodity)
    with open('Atividade6_output.csv', 'w') as f_out:
        f_out.write('year,category,commodity,trade_usd\n')
        for key in sorted(reduced_data):
            year, category = key
            trade_usd, commodity = reduced_data[key]
            f_out.write(f'{year},{category},{commodity},{trade_usd}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
