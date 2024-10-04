#Número de transações por tipo de fluxo (flow) e ano.

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
        mapped_data.append(((flow, year), 1))
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for key, count in mapped_data:
        combined_data[key] = combined_data.get(key, 0) + count
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for key, count in combined_data:
        reduced_data[key] = reduced_data.get(key, 0) + count
    with open('Atividade3_output.csv', 'w') as f_out:
        f_out.write('flow,year,count\n')
        for (flow, year), count in sorted(reduced_data.items()):
            f_out.write(f'{flow},{year},{count}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
