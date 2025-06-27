# etl_transkrip_postgres_final.py
import os
import re
import logging
from logging import FileHandler
import io
import pdfplumber
import psycopg2

logging.basicConfig(
    handlers=[logging.FileHandler("etl_transkrip_postgres.log", mode='w', encoding='utf-8')],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_NAME = "dlh_transkrip_kelasc"
DB_CONFIG = {
    "host": "localhost",
    "user": "postgres",
    "password": "kevin2112",  # ganti jika perlu
    "port": 5432
}

NILAI_BOBOT = {
    "A": 4.0, "AB": 3.5, "B": 3.0, "BC": 2.5, "C": 2.0, "D": 1.0, "E": 0.0
}

# Inisialisasi database
conn_init = psycopg2.connect(**DB_CONFIG, dbname="postgres")
conn_init.autocommit = True
cur_init = conn_init.cursor()
cur_init.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
cur_init.execute(f"CREATE DATABASE {DB_NAME}")
cur_init.close()
conn_init.close()

conn = psycopg2.connect(**DB_CONFIG, dbname=DB_NAME)
cursor = conn.cursor()
logging.info("Database dw berhasil dibuat ulang.")

# Buat tabel
table_sql = [
    """
    CREATE TABLE Dim_Mahasiswa (
        id_mahasiswa SERIAL PRIMARY KEY,
        nrp VARCHAR(20) UNIQUE NOT NULL,
        nama VARCHAR(100) NOT NULL,
        status VARCHAR(50),
        ipk NUMERIC(3,2),
        sks_persiapan INT,
        ip_persiapan NUMERIC(3,2),
        sks_sarjana INT,
        ip_sarjana NUMERIC(3,2),
        sks_tempuh INT,
        sks_lulus INT
    )
    """,
    """
    CREATE TABLE Dim_MataKuliah (
        id_mk SERIAL PRIMARY KEY,
        kode_mk VARCHAR(20) UNIQUE NOT NULL,
        nama_mk VARCHAR(100) NOT NULL,
        sks INT NOT NULL,
        tahap VARCHAR(20) NOT NULL
    )
    """,
    """
    CREATE TABLE Dim_Waktu (
        id_waktu SERIAL PRIMARY KEY,
        tahun INT NOT NULL,
        semester VARCHAR(10) NOT NULL,
        CONSTRAINT unique_time UNIQUE (tahun, semester)
    )
    """,
    """
    CREATE TABLE Dim_Nilai (
        id_nilai SERIAL PRIMARY KEY,
        huruf VARCHAR(5) UNIQUE NOT NULL,
        bobot NUMERIC(3,2) NOT NULL
    )
    """,
    """
    CREATE TABLE Fact_Transkrip (
        id_transkrip SERIAL PRIMARY KEY,
        id_mahasiswa INT REFERENCES Dim_Mahasiswa(id_mahasiswa),
        id_mk INT REFERENCES Dim_MataKuliah(id_mk),
        id_waktu INT REFERENCES Dim_Waktu(id_waktu),
        id_nilai INT REFERENCES Dim_Nilai(id_nilai),
        bobot_matkul NUMERIC(4,2) NOT NULL,
        CONSTRAINT unique_transkrip UNIQUE (id_mahasiswa, id_mk, id_waktu, id_nilai)
    )
    """
]

for sql in table_sql:
    cursor.execute(sql)
    logging.info("Tabel-tabel star schema berhasil dibuat.")

# Insert nilai referensi
for huruf, bobot in NILAI_BOBOT.items():
    cursor.execute("INSERT INTO Dim_Nilai (huruf, bobot) VALUES (%s, %s)", (huruf, bobot))
    logging.info("Referensi nilai berhasil dimasukkan ke Dim_Nilai.")

# Fungsi untuk mendapatkan atau membuat ID
def get_or_create_id(sql_select, sql_insert, select_params, insert_params, returning_field):
    cursor.execute(sql_select, select_params)
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(sql_insert + f" RETURNING {returning_field}", insert_params)
    result = cursor.fetchone()
    return result[0] if result else None

# Mulai proses ETL
folder_path = "data_transkrip"
pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
logging.info(f"📦[INFO]: Ditemukan {len(pdf_files)} file PDF di folder '{folder_path}'\n")

for file in pdf_files:
    try:
        # Extract file PDF
        with pdfplumber.open(os.path.join(folder_path, file)) as pdf:
            text = "\n".join(page.extract_text() for page in pdf.pages)
            logging.info(f"🔄 Memulai proses ETL untuk: {file}")

        # Transform data
        match_nrp_nama = re.search(r"NRP\s*/\s*Nama\s*(\d+)\s*/\s*(.*?)\s*SKS Tempuh", text, re.DOTALL)
        if not match_nrp_nama:
            logging.error(f"❌[GAGAL]: {file} gagal di-transform: NRP/Nama tidak ditemukan.")
            continue

        nrp = match_nrp_nama.group(1).strip()
        nama = match_nrp_nama.group(2).strip()
        ipk = float(re.search(r"IPK\s+(\d+\.\d+)", text).group(1)) if re.search(r"IPK\s+(\d+\.\d+)", text) else 0.0
        status_match = re.search(r"Status\s+(.*?)---", text, re.DOTALL)
        status = status_match.group(1).strip() if status_match else "-"
        ip_persiapan = float(re.search(r"IP Tahap Persiapan\s*:\s*(\d+\.\d+)", text).group(1)) if re.search(r"IP Tahap Persiapan\s*:\s*(\d+\.\d+)", text) else 0.0
        ip_sarjana = float(re.search(r"IP Tahap Sarjana\s*:\s*(\d+\.\d+)", text).group(1)) if re.search(r"IP Tahap Sarjana\s*:\s*(\d+\.\d+)", text) else 0.0
        sks_match = re.search(r"SKS\s*Tempuh\s*/\s*SKS\s*Lulus\s*(\d+)\s*/\s*(\d+)", text)
        sks_tempuh = int(sks_match.group(1)) if sks_match else 0
        sks_lulus = int(sks_match.group(2)) if sks_match else 0
        sks_persiapan_match = re.search(r"Total Sks Tahap Persiapan\s*:\s*(\d+)", text, re.IGNORECASE)
        sks_persiapan = int(sks_persiapan_match.group(1)) if sks_persiapan_match else 0
        sks_sarjana_match = re.search(r"Total Sks Tahap Sarjana\s*:\s*(\d+)", text, re.IGNORECASE)
        sks_sarjana = int(sks_sarjana_match.group(1)) if sks_sarjana_match else 0


        logging.info(f"✅[SUKSES]: {file} berhasil di-transform.")

        # Load data ke database
        id_mhs = get_or_create_id(
            "SELECT id_mahasiswa FROM Dim_Mahasiswa WHERE nrp = %s",
            "INSERT INTO Dim_Mahasiswa (nrp, nama, status, ipk, sks_persiapan, ip_persiapan, sks_sarjana, ip_sarjana, sks_tempuh, sks_lulus) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (nrp,),
            (nrp, nama, status, ipk, sks_persiapan, ip_persiapan, sks_sarjana, ip_sarjana, sks_tempuh, sks_lulus),
            "id_mahasiswa"
        )

        regex_mk = r"([A-Z]{2}\d{6})\s+(.+?)\s+(\d)\s+(\d{4})/(Gs|Gn)/[A-Z]{0,2}\s+([A-Z]{1,2})"
        matches = re.findall(regex_mk, text)

        for kode_mk, nama_mk, sks, tahun, semester_kode, nilai in matches:
            tahap = "Sarjana" if "Tahap: Sarjana" in text and text.index("Tahap: Sarjana") < text.index(kode_mk) else "Persiapan"
            semester = "Gasal" if semester_kode == "Gs" else "Genap"
            sks = int(sks)
            bobot = NILAI_BOBOT.get(nilai, 0.0)
            bobot_matkul = bobot * sks  # Bobot matkul dihitung sebagai bobot nilai dikali SKS

            id_mk = get_or_create_id(
                "SELECT id_mk FROM Dim_MataKuliah WHERE kode_mk = %s",
                "INSERT INTO Dim_MataKuliah (kode_mk, nama_mk, sks, tahap) VALUES (%s, %s, %s, %s)",
                (kode_mk,),
                (kode_mk, nama_mk.strip(), int(sks), tahap),
                "id_mk"
            )

            id_waktu = get_or_create_id(
                "SELECT id_waktu FROM Dim_Waktu WHERE tahun = %s AND semester = %s",
                "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)",
                (int(tahun), semester),
                (int(tahun), semester),
                "id_waktu"
            )

            id_nilai = get_or_create_id(
                "SELECT id_nilai FROM Dim_Nilai WHERE huruf = %s",
                "INSERT INTO Dim_Nilai (huruf, bobot) VALUES (%s, %s)",
                (nilai,),
                (nilai, NILAI_BOBOT.get(nilai, 0.0)),
                "id_nilai"
            )

            cursor.execute(
                "INSERT INTO Fact_Transkrip (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul) VALUES (%s, %s, %s, %s, %s)",
                (id_mhs, id_mk, id_waktu, id_nilai, bobot_matkul)
            )

        logging.info(f"🎉[SUKSES]: Proses ETL untuk {file} SELESAI.\n")

    except Exception as e:
        conn.rollback()
        logging.error(f"💥[ERROR]: {file} error fatal: {e}\n")

conn.commit()
cursor.close()
conn.close()
print("✅ Seluruh proses ETL PostgreSQL selesai. Lihat log di etl_transkrip_postgres.log")
