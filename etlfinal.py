import os
import re
import pdfplumber
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# === KONFIGURASI DATABASE POSTGRESQL ===
DB_CONFIG = {
    'user': 'postgres',
    'password': 'kevin2112',
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'dw_nilai_akademik'
}
DB_URI = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
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

def is_file_processed(conn, filename):
    result = conn.execute(text("SELECT 1 FROM processed_files_log WHERE file_name = :fname"), {'fname': filename})
    return result.scalar() is not None

def log_processed_file(conn, filename):
    conn.execute(text("INSERT INTO processed_files_log (file_name) VALUES (:fname) ON CONFLICT DO NOTHING"), {'fname': filename})

def insert_data(data, filename):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_mahasiswa (nrp, nama_mahasiswa, sks_tempuh_total, sks_lulus_total, status_mahasiswa)
            VALUES (:nrp, :nama_mahasiswa, :sks_tempuh_total, :sks_lulus_total, :status)
            ON CONFLICT (nrp) DO UPDATE SET nama_mahasiswa = EXCLUDED.nama_mahasiswa
        """), {
            'nrp': data['nrp'],
            'nama_mahasiswa': data['nama'],
            'sks_tempuh_total': data['sks_tempuh'],
            'sks_lulus_total': data['sks_lulus'],
            'status': 'Aktif'
        })

        mhs_id = conn.execute(text("SELECT id_mahasiswa FROM dim_mahasiswa WHERE nrp = :nrp"), {'nrp': data['nrp']}).fetchone()[0]

        for g in data['grades']:
            conn.execute(text("""
                INSERT INTO dim_waktu (tahun_akademik, periode_akademik, tahap_akademik)
                VALUES (:tahun, :periode, :tahap)
                ON CONFLICT (tahun_akademik, periode_akademik, tahap_akademik) DO NOTHING
            """), {
                'tahun': g['tahun'],
                'periode': g['periode'],
                'tahap': 'Auto'
            })

            waktu_id = conn.execute(text("""
                SELECT id_waktu FROM dim_waktu
                WHERE tahun_akademik = :tahun AND periode_akademik = :periode AND tahap_akademik = :tahap
            """), {
                'tahun': g['tahun'],
                'periode': g['periode'],
                'tahap': 'Auto'
            }).fetchone()[0]

            conn.execute(text("""
                INSERT INTO dim_matakuliah (kode_mata_kuliah, nama_mata_kuliah, sks_mata_kuliah_def)
                VALUES (:kode, :nama, :sks)
                ON CONFLICT (kode_mata_kuliah) DO UPDATE SET nama_mata_kuliah = EXCLUDED.nama_mata_kuliah
            """), {
                'kode': g['kode_mk'],
                'nama': g['nama_mk'],
                'sks': g['sks']
            })

            mk_id = conn.execute(text("SELECT id_mata_kuliah FROM dim_matakuliah WHERE kode_mata_kuliah = :kode"), {
                'kode': g['kode_mk']
            }).fetchone()[0]

            conn.execute(text("""
                INSERT INTO fact_nilai_akademik (
                    id_mahasiswa, id_waktu, id_mata_kuliah,
                    nilai_huruf, sks_mata_kuliah_earned, bobot_nilai_numeric,
                    ip_tahap_saat_ini, ipk_kumulatif_saat_ini, status_kelulusan_mk
                ) VALUES (:mhs_id, :waktu_id, :mk_id, :nilai, :sks, :bobot, :ip_tahap, :ipk, :lulus)
            """), {
                'mhs_id': mhs_id,
                'waktu_id': waktu_id,
                'mk_id': mk_id,
                'nilai': g['nilai'],
                'sks': g['sks'],
                'bobot': g['bobot'],
                'ip_tahap': None,
                'ipk': data['ipk'],
                'lulus': g['lulus']
            })

        log_processed_file(conn, filename)

def run_historical():
    print("\n🚀 Memulai ETL HISTORICAL...")
    for filename in os.listdir(PDF_DIR):
        if filename.endswith(".pdf"):
            print(f"\n🔍 Parsing file: {filename}")
            path = os.path.join(PDF_DIR, filename)
            try:
                parsed = parse_transkrip(path)
                insert_data(parsed, filename)
                print(f"✅ Data dari {filename} berhasil dimuat.")
            except Exception as e:
                print(f"❌ Gagal memproses {filename}: {e}")

def run_incremental():
    print("\n🚀 Memulai ETL INCREMENTAL...")
    with engine.begin() as conn:
        for filename in os.listdir(PDF_DIR):
            if filename.endswith(".pdf"):
                if is_file_processed(conn, filename):
                    print(f"⏭️  Lewati {filename} (sudah diproses)")
                    continue
                print(f"\n🔍 Parsing file baru: {filename}")
                try:
                    parsed = parse_transkrip(os.path.join(PDF_DIR, filename))
                    insert_data(parsed, filename)
                    print(f"✅ File baru {filename} berhasil dimuat.")
                except Exception as e:
                    print(f"❌ Gagal memproses {filename}: {e}")

if __name__ == "__main__":
    if not os.path.exists(PDF_DIR):
        os.makedirs(PDF_DIR)
        print(f"📁 Folder '{PDF_DIR}' dibuat. Silakan letakkan file PDF transkrip di sana.")
        exit()

    mode = input("\nMode ETL [historical/incremental]: ").strip().lower()
    if mode == "historical":
        run_historical()
    elif mode == "incremental":
        run_incremental()
    else:
        print("❌ Mode tidak dikenal. Gunakan 'historical' atau 'incremental'.")
