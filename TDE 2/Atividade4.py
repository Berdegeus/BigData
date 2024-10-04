#Valor médio das transações por ano.

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
        trade_usd = fields[5]
        try:
            trade_usd = float(trade_usd)
            mapped_data.append((year, (trade_usd, 1)))
        except ValueError:
            continue  # Skip lines with invalid trade_usd
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for year, (trade_usd, count) in mapped_data:
        total_trade_usd, total_count = combined_data.get(year, (0.0, 0))
        combined_data[year] = (total_trade_usd + trade_usd, total_count + count)
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for year, (trade_usd, count) in combined_data:
        total_trade_usd, total_count = reduced_data.get(year, (0.0, 0))
        reduced_data[year] = (total_trade_usd + trade_usd, total_count + count)
    with open('Atividade4_output.csv', 'w') as f_out:
        f_out.write('year,average_trade_usd\n')
        for year in sorted(reduced_data):
            total_trade_usd, total_count = reduced_data[year]
            if total_count > 0:
                avg = total_trade_usd / total_count
                f_out.write(f'{year},{avg}\n')
            else:
                f_out.write(f'{year},0\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
