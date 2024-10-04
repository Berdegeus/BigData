# Descrição e valor por quilograma da commodity mais lucrativa por peso
# comercializada em 2015, por tipo de fluxo. Considerar somente transações com Number of
# items, em que é possível calcular o peso por unidade e o valor por quilo.

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
        flow = fields[4]
        commodity = fields[3]
        trade_usd = fields[5]
        weight_kg = fields[6]
        quantity_name = fields[7]
        quantity = fields[8]
        if (year == '2015' and quantity_name == 'Number of items' and
            weight_kg != '' and quantity != '' and trade_usd != ''):
            try:
                trade_usd = float(trade_usd)
                weight_kg = float(weight_kg)
                quantity = float(quantity)
                if weight_kg > 0 and quantity > 0:
                    value_per_kg = trade_usd / weight_kg
                    mapped_data.append(((flow, commodity), value_per_kg))
            except ValueError:
                continue  # Skip lines with invalid numerical values
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for key, value_per_kg in mapped_data:
        max_value_per_kg = combined_data.get(key, 0.0)
        if value_per_kg > max_value_per_kg:
            combined_data[key] = value_per_kg
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    max_values = {}
    for key, value_per_kg in combined_data:
        flow, commodity = key
        max_value_per_kg, max_commodity = max_values.get(flow, (0.0, ''))
        if value_per_kg > max_value_per_kg:
            max_values[flow] = (value_per_kg, commodity)
    with open('Atividade8_output.csv', 'w') as f_out:
        f_out.write('flow,commodity,value_per_kg\n')
        for flow in sorted(max_values):
            value_per_kg, commodity = max_values[flow]
            f_out.write(f'{flow},{commodity},{value_per_kg}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
