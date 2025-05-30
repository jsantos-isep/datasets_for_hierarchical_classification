import csv
import json

csv_file_path = '/mnt/c/Users/joana/Desktop/PhD/datasets_for_hierarchical_classification/the_guardian/the_guardian_val.csv'
json_file_path = '/mnt/c/Users/joana/Desktop/PhD/datasets_for_hierarchical_classification/the_guardian/the_guardian_val.json'

with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csv_file:
    reader = csv.DictReader(csv_file, delimiter=';')

    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json_file.write('[\n')
        first = True
        for row in reader:
            if not first:
                json_file.write(',\n')
            json.dump(row, json_file)
            first = False
        json_file.write('\n]')