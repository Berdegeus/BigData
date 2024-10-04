# Descrição e quantidade total de itens da commodity mais comercializada em 2014
# pela China, por tipo de fluxo. Considerar somente transações com Number of items.

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
        commodity = fields[3]
        flow = fields[4]
        quantity_name = fields[7]
        quantity = fields[8]
        if country == 'China' and year == '2014' and quantity_name == 'Number of items':
            try:
                quantity = float(quantity)
                mapped_data.append(((flow, commodity), quantity))
            except ValueError:
                continue  # Skip lines with invalid quantity
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for key, quantity in mapped_data:
        total_quantity = combined_data.get(key, 0.0)
        combined_data[key] = total_quantity + quantity
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    total_quantities = {}
    for key, quantity in combined_data:
        total_quantity = total_quantities.get(key, 0.0)
        total_quantities[key] = total_quantity + quantity
    max_quantities = {}
    for (flow, commodity), quantity in total_quantities.items():
        max_quantity, max_commodity = max_quantities.get(flow, (0.0, ''))
        if quantity > max_quantity:
            max_quantities[flow] = (quantity, commodity)
    with open('Atividade7_output.csv', 'w') as f_out:
        f_out.write('flow,commodity,total_quantity\n')
        for flow in sorted(max_quantities):
            quantity, commodity = max_quantities[flow]
            f_out.write(f'{flow},{commodity},{quantity}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
