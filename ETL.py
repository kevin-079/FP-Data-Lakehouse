import os
import pdfplumber
import re
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# === KONFIGURASI DATABASE ===
DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'dw_nilai_akademik'
}
DB_URI = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URI)

PDF_DIR = 'data_transkrip'

def calculate_grade_metrics(nilai_huruf):
    bobot = {
        'A': 4.0, 'AB': 3.5, 'B': 3.0, 'BC': 2.5,
        'C': 2.0, 'D': 1.0, 'E': 0.0
    }.get(nilai_huruf, 0.0)
    lulus = nilai_huruf in ['A', 'AB', 'B', 'BC', 'C']
    return bobot, lulus

def parse_transkrip(file_path):
    with pdfplumber.open(file_path) as pdf:
        text_pdf = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    data = {
        'nrp': re.search(r"NRP\s*\/\s*Nama\s*(\d+)", text_pdf).group(1),
        'nama': re.search(r"\/\s*Nama\s*\d+\s*\/\s*(.*?)\n", text_pdf).group(1).strip(),
        'ipk': float(re.search(r"IPK\s*\n?(\d+\.\d+)", text_pdf).group(1)),
        'sks_tempuh': int(re.search(r"SKS Tempuh\s*\/\s*SKS Lulus\s*(\d+)\s*\/", text_pdf).group(1)),
        'sks_lulus': int(re.search(r"\/\s*(\d+)\s*\nStatus", text_pdf).group(1)),
        'grades': []
    }

    pattern = re.compile(r"([A-Z]{2}\d{6})\s+(.+?)\s+(\d+)\s+(\d{4}/[A-Za-z]{2}/[A-Z]{1,2})\s+([A-Z]{1,2})")
    for match in pattern.finditer(text_pdf):
        kode, nama, sks, histori, nilai = match.groups()
        tahun, periode, _ = histori.split("/")
        bobot, lulus = calculate_grade_metrics(nilai)
        data['grades'].append({
            'kode_mk': kode,
            'nama_mk': nama,
            'sks': int(sks),
            'tahun': int(tahun),
            'periode': periode,
            'nilai': nilai,
            'bobot': bobot,
            'lulus': lulus
        })
    return data

def insert_data(data):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_mahasiswa (nrp, nama_mahasiswa, sks_tempuh_total, sks_lulus_total, status_mahasiswa)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE nama_mahasiswa = VALUES(nama_mahasiswa)
        """), [(data['nrp'], data['nama'], data['sks_tempuh'], data['sks_lulus'], 'Aktif')])

        mhs_id = conn.execute(text("SELECT id_mahasiswa FROM dim_mahasiswa WHERE nrp = %s"), [(data['nrp'],)]).fetchone()[0]

        for g in data['grades']:
            conn.execute(text("""
                INSERT INTO dim_waktu (tahun_akademik, periode_akademik, tahap_akademik)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE tahun_akademik = tahun_akademik
            """), [(g['tahun'], g['periode'], 'Auto')])

            waktu_id = conn.execute(text("""
                SELECT id_waktu FROM dim_waktu
                WHERE tahun_akademik = %s AND periode_akademik = %s AND tahap_akademik = %s
            """), [(g['tahun'], g['periode'], 'Auto')]).fetchone()[0]

            conn.execute(text("""
                INSERT INTO dim_matakuliah (kode_mata_kuliah, nama_mata_kuliah, sks_mata_kuliah_def)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE nama_mata_kuliah = VALUES(nama_mata_kuliah)
            """), [(g['kode_mk'], g['nama_mk'], g['sks'])])

            mk_id = conn.execute(text("SELECT id_mata_kuliah FROM dim_matakuliah WHERE kode_mata_kuliah = %s"), [(g['kode_mk'],)]).fetchone()[0]

            conn.execute(text("""
                INSERT INTO fact_nilai_akademik (
                    id_mahasiswa, id_waktu, id_mata_kuliah,
                    nilai_huruf, sks_mata_kuliah_earned, bobot_nilai_numeric,
                    ip_tahap_saat_ini, ipk_kumulatif_saat_ini, status_kelulusan_mk
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """), [(mhs_id, waktu_id, mk_id, g['nilai'], g['sks'], g['bobot'], None, data['ipk'], g['lulus'])])

if __name__ == "__main__":
    for filename in os.listdir(PDF_DIR):
        if filename.endswith(".pdf"):
            print(f"🔍 Parsing {filename}...")
            parsed = parse_transkrip(os.path.join(PDF_DIR, filename))
            insert_data(parsed)
            print(f"✅ Data dari {filename} berhasil dimasukkan.")
