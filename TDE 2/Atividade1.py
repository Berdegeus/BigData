#Número de transações envolvendo Australia, Brasil e China.

import sys

def mapper(input_data):
    mapped_data = []
    for line in input_data:
        line = line.strip()
        if line.startswith('country_or_area;'):
            continue  # Ignore the header
        fields = line.split(';')
        if len(fields) != 10:
            continue  # Ignore invalid lines
        country = fields[0]
        if country in ['Australia', 'Brazil', 'China']:
            mapped_data.append((country, 1))
    return mapped_data

def combiner(mapped_data):
    combined_data = {}
    for country, count in mapped_data:
        combined_data[country] = combined_data.get(country, 0) + count
    combined_list = list(combined_data.items())
    return combined_list

def reducer(combined_data):
    reduced_data = {}
    for country, count in combined_data:
        reduced_data[country] = reduced_data.get(country, 0) + count
    with open('Atividade1_output.csv', 'w') as f_out:
        f_out.write('country,count\n')
        for country, count in reduced_data.items():
            f_out.write(f'{country},{count}\n')

if __name__ == '__main__':
    input_data = sys.stdin.readlines()
    mapped = mapper(input_data)
    combined = combiner(mapped)
    reducer(combined)
