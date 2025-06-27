# etl_transkrip.py
import os
import re
import logging
from logging import FileHandler
import io
import pdfplumber
import mysql.connector


# === Konfigurasi Logging ===
logging.basicConfig(
    handlers=[logging.FileHandler("ETL_Transkrip.log", mode='w', encoding='utf-8')],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Konfigurasi Database ===
DB_NAME = "dlh_transkrip_2fact"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "port": 3306
}

NILAI_BOBOT = {
    "A": 4.0,
    "AB": 3.5,
    "B": 3.0,
    "BC": 2.5,
    "C": 2.0,
    "D": 1.0,
    "E": 0.0
}

# === Buat Koneksi Awal ke MySQL ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Buat dan Refresh Database ===
cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
cursor.execute(f"CREATE DATABASE {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")
logging.info("Database dw berhasil dibuat ulang.")

# === Buat Tabel-Tabel Star Schema ===
table_sql = [
    """
    CREATE TABLE Dim_Mahasiswa (
        id_mahasiswa INT AUTO_INCREMENT PRIMARY KEY,
        nrp VARCHAR(20) UNIQUE NOT NULL,
        nama VARCHAR(100) NOT NULL,
        status VARCHAR(50),
        ipk DECIMAL(3,2),
        sks_persiapan INT,
        ip_persiapan DECIMAL(3,2),
        sks_sarjana INT,
        ip_sarjana DECIMAL(3,2),
        sks_tempuh INT,
        sks_lulus INT
    )
    """,
    """
    CREATE TABLE Dim_MataKuliah (
        id_mk INT AUTO_INCREMENT PRIMARY KEY,
        kode_mk VARCHAR(20) UNIQUE NOT NULL,
        nama_mk VARCHAR(100) NOT NULL,
        sks INT NOT NULL,
        tahap VARCHAR(20) NOT NULL
    )
    """,
    """
    CREATE TABLE Dim_Waktu (
        id_waktu INT AUTO_INCREMENT PRIMARY KEY,
        tahun INT NOT NULL,
        semester VARCHAR(20) NOT NULL,
        UNIQUE KEY unique_time (tahun, semester)
    )
    """,
    """
    CREATE TABLE Dim_Nilai (
        id_nilai INT AUTO_INCREMENT PRIMARY KEY,
        huruf VARCHAR(5) UNIQUE NOT NULL,
        bobot DECIMAL(3,2) NOT NULL
    )
    """,
    """
    CREATE TABLE Fact_Nilai_MK (
        id_transkrip INT AUTO_INCREMENT PRIMARY KEY,
        id_mahasiswa INT NOT NULL,
        id_mk INT  NOT NULL,
        id_waktu INT NOT NULL,
        id_nilai INT NOT NULL,
        bobot_matkul DECIMAL (4,2) NOT NULL,
        FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
        FOREIGN KEY (id_mk) REFERENCES Dim_MataKuliah(id_mk),
        FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
        FOREIGN KEY (id_nilai) REFERENCES Dim_Nilai(id_nilai),
        UNIQUE KEY unique_transkrip (id_mahasiswa, id_mk, id_waktu, id_nilai)
    )
    """,
    """
    CREATE TABLE Fact_Nilai_Semester (
        id_fakta INT AUTO_INCREMENT PRIMARY KEY,
        id_mahasiswa INT NOT NULL,
        id_waktu INT NOT NULL,
        id_nilai INT NOT NULL,
        ips DECIMAL(3,2) NOT NULL,
        ipk DECIMAL(3,2) NOT NULL,
        FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
        FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
        FOREIGN KEY (id_nilai) REFERENCES Dim_Nilai(id_nilai)
    )
    """
]

for sql in table_sql:
    cursor.execute(sql)
logging.info("Tabel-tabel star schema berhasil dibuat.")

# === Insert Nilai Referensi ===
for huruf, bobot in NILAI_BOBOT.items():
    cursor.execute("INSERT INTO Dim_Nilai (huruf, bobot) VALUES (%s, %s)", (huruf, bobot))
logging.info("Referensi nilai berhasil dimasukkan ke Dim_Nilai.")

# === Fungsi Insert Helper ===
def get_or_create_id(sql_select, sql_insert, select_params, insert_params):
    cursor.execute(sql_select, select_params)
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(sql_insert, insert_params)
    return cursor.lastrowid

# === Proses Semua PDF ===
folder_path = "data_transkrip"
pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
logging.info(f"📦 [INFO]: Ditemukan {len(pdf_files)} file PDF di folder '{folder_path}'\n")

for file in pdf_files:
    try:
        # === Extract Data dari PDF ===
        with pdfplumber.open(os.path.join(folder_path, file)) as pdf:
            text = "\n".join(page.extract_text() for page in pdf.pages)
            logging.info(f"🔄 Memulai proses ETL untuk: {file}")

        # === Transform Data ===
        match_nrp_nama = re.search(r"NRP\s*/\s*Nama\s*(\d+)\s*/\s*(.*?)\s*SKS Tempuh", text, re.DOTALL)
        if not match_nrp_nama:
            logging.error(f"❌ [GAGAL]: {file} gagal di-transform: NRP/Nama tidak ditemukan.")
            continue

        nrp = match_nrp_nama.group(1).strip()
        nama = match_nrp_nama.group(2).strip()

        ipk_match = re.search(r"IPK\s+(\d+\.\d+)", text)
        ipk = float(ipk_match.group(1)) if ipk_match else 0.0

        status_match = re.search(r"Status\s+(.*?)---", text, re.DOTALL)
        status = status_match.group(1).strip() if status_match else "-"

        ip_persiapan_match = re.search(r"IP Tahap Persiapan\s*:\s*(\d+\.\d+)", text)
        ip_persiapan = float(ip_persiapan_match.group(1)) if ip_persiapan_match else 0.0

        ip_sarjana_match = re.search(r"IP Tahap Sarjana\s*:\s*(\d+\.\d+)", text)
        ip_sarjana = float(ip_sarjana_match.group(1)) if ip_sarjana_match else 0.0

        sks_match = re.search(r"SKS\s*Tempuh\s*/\s*SKS\s*Lulus\s*(\d+)\s*/\s*(\d+)", text)
        sks_tempuh = int(sks_match.group(1)) if sks_match else 0
        sks_lulus = int(sks_match.group(2)) if sks_match else 0

        sks_persiapan_match = re.search(r"Total Sks Tahap Persiapan\s*:\s*(\d+)", text, re.IGNORECASE)
        sks_persiapan = int(sks_persiapan_match.group(1)) if sks_persiapan_match else 0

        sks_sarjana_match = re.search(r"Total Sks Tahap Sarjana\s*:\s*(\d+)", text, re.IGNORECASE)
        sks_sarjana = int(sks_sarjana_match.group(1)) if sks_sarjana_match else 0

        logging.info(f"✅ [SUKSES]: {file} berhasil di-transform.")

        # === Load Data ke Tabel DW ===
        id_mhs = get_or_create_id(
            "SELECT id_mahasiswa FROM Dim_Mahasiswa WHERE nrp = %s",
            "INSERT INTO Dim_Mahasiswa (nrp, nama, status, ipk, sks_persiapan, ip_persiapan, sks_sarjana, ip_sarjana, sks_tempuh, sks_lulus) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (nrp,),
            (nrp, nama, status, ipk, sks_persiapan, ip_persiapan, sks_sarjana, ip_sarjana, sks_tempuh, sks_lulus)
        )

        regex_mk = r"([A-Z]{2}\d{6})\s+(.+?)\s+(\d)\s+(\d{4})/(Gs|Gn)/[A-Z]{0,2}\s+([A-Z]{1,2})"
        matches = re.findall(regex_mk, text)

        for kode_mk, nama_mk, sks, tahun, semester_kode, nilai in matches:
            tahap = "Sarjana" if "Tahap: Sarjana" in text and text.index("Tahap: Sarjana") < text.index(kode_mk) else "Persiapan"
            semester = "Gasal" if semester_kode == "Gs" else "Genap"
            sks_int = int(sks)
            bobot = NILAI_BOBOT.get(nilai, 0.0)
            bobot_matkul = sks_int * bobot

            id_mk = get_or_create_id(
                "SELECT id_mk FROM Dim_MataKuliah WHERE kode_mk = %s",
                "INSERT INTO Dim_MataKuliah (kode_mk, nama_mk, sks, tahap) VALUES (%s, %s, %s, %s)",
                (kode_mk,),
                (kode_mk, nama_mk.strip(), int(sks), tahap)
            )

            id_waktu = get_or_create_id(
                "SELECT id_waktu FROM Dim_Waktu WHERE tahun = %s AND semester = %s",
                "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)",
                (int(tahun), semester),
                (int(tahun), semester)
            )

            id_nilai = get_or_create_id(
                "SELECT id_nilai FROM Dim_Nilai WHERE huruf = %s",
                "INSERT INTO Dim_Nilai (huruf, bobot) VALUES (%s, %s)",
                (nilai,),
                (nilai, NILAI_BOBOT.get(nilai, 0.0))
            )

            cursor.execute(
                "INSERT INTO Fact_Nilai_MK (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul) VALUES (%s, %s, %s, %s, %s)",
                (id_mhs, id_mk, id_waktu, id_nilai, bobot_matkul)
            )
            # === Hitung dan Masukkan Data ke Fact_Nilai_Semester ===
            cursor.execute("SELECT DISTINCT id_mahasiswa FROM Fact_Nilai_MK")
            mahasiswa_list = cursor.fetchall()

            for (id_mahasiswa,) in mahasiswa_list:
                cursor.execute("""
                    SELECT DISTINCT id_waktu FROM Fact_Nilai_MK
                    WHERE id_mahasiswa = %s
                    ORDER BY id_waktu
                """, (id_mahasiswa,))
                
                waktu_list = cursor.fetchall()
                ipk_sebelumnya = 0.0
                total_sks_kumulatif = 0
                total_bobot_kumulatif = 0

                for (id_waktu,) in waktu_list:
                    # Ambil semua MK di semester itu
                    cursor.execute("""
                        SELECT dmk.sks, dn.bobot
                        FROM Fact_Nilai_MK fn
                        JOIN Dim_MataKuliah dmk ON fn.id_mk = dmk.id_mk
                        JOIN Dim_Nilai dn ON fn.id_nilai = dn.id_nilai
                        WHERE fn.id_mahasiswa = %s AND fn.id_waktu = %s
                    """, (id_mahasiswa, id_waktu))

                    nilai_sks_list = cursor.fetchall()

                    total_sks_semester = sum(sks for sks, _ in nilai_sks_list)
                    total_bobot_semester = sum(sks * bobot for sks, bobot in nilai_sks_list)
                    ips = round(total_bobot_semester / total_sks_semester, 2) if total_sks_semester > 0 else 0.0

                    # Update kumulatif untuk IPK
                    total_sks_kumulatif += total_sks_semester
                    total_bobot_kumulatif += total_bobot_semester
                    ipk = round(total_bobot_kumulatif / total_sks_kumulatif, 2) if total_sks_kumulatif > 0 else 0.0

                    # Ambil nilai semester (untuk representasi dominan)
                    cursor.execute("""
                        SELECT id_nilai FROM Fact_Nilai_MK
                        WHERE id_mahasiswa = %s AND id_waktu = %s
                        ORDER BY id_nilai DESC LIMIT 1
                    """, (id_mahasiswa, id_waktu))
                    result = cursor.fetchone()
                    id_nilai = result[0] if result else None

                    cursor.execute("""
                        INSERT INTO Fact_Nilai_Semester (id_mahasiswa, id_waktu, id_nilai, ips, ipk)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (id_mahasiswa, id_waktu, id_nilai, ips, ipk))

        logging.info(f"🎉[SUKSES]: Proses ETL untuk {file} SELESAI.\n")

    except Exception as e:
        logging.error(f"💥 [ERROR]: {file} error fatal: {e}\n")
conn.commit()
cursor.close()
conn.close()
print("✅ Seluruh proses ETL selesai. Lihat log di ETL_Transkrip.log")