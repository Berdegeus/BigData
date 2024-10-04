# Valor médio das transações por categoria, ano e tipo de unidade considerando
# somente as transações do tipo exportação realizadas no Brasil.

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
        country = fields[0]
        year = fields[1]
        flow = fields[4]
        trade_usd = fields[5]
        quantity_name = fields[7]
        category = fields[9]
        if country == 'Brazil' and flow == 'Export':
            try:
                trade_usd = float(trade_usd)
                mapped_data.append(((category, year, quantity_name), (trade_usd, 1)))
            except ValueError:
                continue  # Skip lines with invalid trade_usd
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for key, (trade_usd, count) in mapped_data:
        total_trade_usd, total_count = combined_data.get(key, (0.0, 0))
        combined_data[key] = (total_trade_usd + trade_usd, total_count + count)
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for key, (trade_usd, count) in combined_data:
        total_trade_usd, total_count = reduced_data.get(key, (0.0, 0))
        reduced_data[key] = (total_trade_usd + trade_usd, total_count + count)
    with open('Atividade5_output.csv', 'w') as f_out:
        f_out.write('category,year,quantity_name,average_trade_usd\n')
        for key in sorted(reduced_data):
            category, year, quantity_name = key
            total_trade_usd, total_count = reduced_data[key]
            if total_count > 0:
                avg = total_trade_usd / total_count
                f_out.write(f'{category},{year},{quantity_name},{avg}\n')
            else:
                f_out.write(f'{category},{year},{quantity_name},0\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
