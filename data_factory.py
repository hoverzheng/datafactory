import argparse
import csv
import json
import yaml
from faker import Faker
from itertools import islice

def load_config(config_file):
    with open(config_file, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def generate_fake_data(config, num_rows, batch_size=10000):
    fake = Faker('zh_CN')  # 指定生成中文内容
    while num_rows > 0:
        current_batch_size = min(batch_size, num_rows)
        data = []
        for _ in range(current_batch_size):
            row = {}
            for field, faker_method in config['fields'].items():
                row[field] = eval(f'fake.{faker_method}()')
            data.append(row)
        yield data
        num_rows -= current_batch_size

def write_csv_header(output_file, delimiter, fieldnames):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()

def write_csv(data, output_file, delimiter):
    with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys(), delimiter=delimiter)
        writer.writerows(data)

def write_json(data, output_file):
    with open(output_file, 'a', encoding='utf-8') as jsonfile:
        for record in data:
            jsonfile.write(json.dumps(record, ensure_ascii=False) + '\n')

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic data based on configuration.')
    parser.add_argument('-c', '--config', required=True, help='Path to the YAML configuration file.')
    parser.add_argument('-o', '--output', required=True, help='Output file name.')
    parser.add_argument('-n', '--num', required=True, type=int, help='Number of rows of data to generate.')
    args = parser.parse_args()

    config = load_config(args.config)
    num_rows = args.num
    output_format = config['output']['format']
    delimiter = config['output'].get('delimiter', ',')
    
    if output_format == 'csv':
        fieldnames = list(config['fields'].keys())
        write_csv_header(args.output, delimiter, fieldnames)
    
    for data_batch in generate_fake_data(config, num_rows):        
        if output_format == 'csv':
            write_csv(data_batch, args.output, delimiter)
        elif output_format == 'json':
            write_json(data_batch, args.output)
        else:
            raise ValueError(f'Unsupported output format: {output_format}')

if __name__ == '__main__':
    main()
