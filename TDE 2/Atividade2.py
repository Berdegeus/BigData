#Número de transações por ano.

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
        mapped_data.append((year, 1))
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for year, count in mapped_data:
        combined_data[year] = combined_data.get(year, 0) + count
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for year, count in combined_data:
        reduced_data[year] = reduced_data.get(year, 0) + count
    with open('Atividade2_output.csv', 'w') as f_out:
        f_out.write('year,count\n')
        for year, count in sorted(reduced_data.items()):
            f_out.write(f'{year},{count}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
