# map.py
import csv

data = []
with open('GCB2022v27_MtCO2_flat.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)  # Skip header
    for row in csv_reader:
        country = row[0]
        total_emission = row[3] if row[3] != '' else '0'  # Handle empty
        data.append([country, total_emission])

with open('intermediate.csv', mode='w') as csv_file:
    csv_writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in data:
        csv_writer.writerow(row)
