# reduce.py
import csv

# Dictionary untuk menyimpan total emisi per negara
total_emisi = {}

with open('intermediate.csv') as file_csv:
    csv_reader = csv.reader(file_csv, delimiter=',')
    for row in csv_reader:
        if not row or len(row) < 2:
            continue
        negara = row[0]
        try:
            emisi = float(row[1])
        except ValueError:
            emisi = 0  # jika data kosong atau error
        total_emisi[negara] = total_emisi.get(negara, 0) + emisi

# Simpan hasil ke result.csv
with open('result.csv', mode='w', newline='') as file_csv:
    csv_writer = csv.writer(file_csv)
    csv_writer.writerow(['Negara', 'Total Emisi'])
    for negara, total in total_emisi.items():
        csv_writer.writerow([negara, round(total, 3)])
