import PyPDF2
import re
import psycopg2
from psycopg2 import extras # Untuk executemany
import os
from datetime import datetime
import pandas as pd

# --- 1. Konfigurasi ---
# Ganti dengan kredensial PostgreSQL Anda yang sebenarnya
DB_CONFIG = {
    'host': 'localhost',
    'database': 'dw_nilai_akademik', # Nama database DW Anda
    'user': 'your_username',
    'password': 'your_password'
}

# Folder tempat menyimpan file PDF transkrip Anda
PDF_DIR = 'data_transkrip' 

# --- 2. Kelas untuk Mengelola Koneksi & Operasi Database ---
class DbManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cur = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            return self
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def get_max_surrogate_key(self, table_name, id_column_name):
        """Mendapatkan ID surrogate terbesar dari tabel dimensi.
           (Kurang relevan jika pakai SERIAL, tapi bisa berguna untuk skenario lain)"""
        try:
            query = f"SELECT COALESCE(MAX({id_column_name}), 0) FROM {table_name};"
            self.cur.execute(query)
            return self.cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error getting max surrogate key for {table_name}: {e}")
            raise

    def insert_or_get_dim_mahasiswa(self, nrp, nama_mahasiswa, sks_tempuh, sks_lulus, status):
        try:
            self.cur.execute(
                """
                INSERT INTO Dim_Mahasiswa (nrp, nama_mahasiswa, sks_tempuh_total, sks_lulus_total, status_mahasiswa)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (nrp) DO UPDATE SET
                    nama_mahasiswa = EXCLUDED.nama_mahasiswa,
                    sks_tempuh_total = EXCLUDED.sks_tempuh_total,
                    sks_lulus_total = EXCLUDED.sks_lulus_total,
                    status_mahasiswa = EXCLUDED.status_mahasiswa,
                    updated_at = NOW()
                RETURNING id_mahasiswa;
                """,
                (nrp, nama_mahasiswa, sks_tempuh, sks_lulus, status)
            )
            return self.cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error inserting/getting Dim_Mahasiswa: {e}")
            raise

    def insert_or_get_dim_waktu(self, tahun_akademik, periode_akademik, tahap_akademik):
        try:
            self.cur.execute(
                """
                INSERT INTO Dim_Waktu (tahun_akademik, periode_akademik, tahap_akademik)
                VALUES (%s, %s, %s)
                ON CONFLICT (tahun_akademik, periode_akademik, tahap_akademik) DO NOTHING
                RETURNING id_waktu;
                """,
                (tahun_akademik, periode_akademik, tahap_akademik)
            )
            waktu_id = self.cur.fetchone()
            if waktu_id:
                return waktu_id[0]
            else: # Jika sudah ada, ambil ID-nya
                self.cur.execute(
                    "SELECT id_waktu FROM Dim_Waktu WHERE tahun_akademik = %s AND periode_akademik = %s AND tahap_akademik = %s",
                    (tahun_akademik, periode_akademik, tahap_akademik)
                )
                return self.cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error inserting/getting Dim_Waktu: {e}")
            raise

    def insert_or_get_dim_matakuliah(self, kode_mata_kuliah, nama_mata_kuliah, sks_def):
        try:
            self.cur.execute(
                """
                INSERT INTO Dim_MataKuliah (kode_mata_kuliah, nama_mata_kuliah, sks_mata_kuliah_def)
                VALUES (%s, %s, %s)
                ON CONFLICT (kode_mata_kuliah) DO UPDATE SET
                    nama_mata_kuliah = EXCLUDED.nama_mata_kuliah,
                    sks_mata_kuliah_def = EXCLUDED.sks_mata_kuliah_def,
                    updated_at = NOW()
                RETURNING id_mata_kuliah;
                """,
                (kode_mata_kuliah, nama_mata_kuliah, sks_def)
            )
            return self.cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error inserting/getting Dim_MataKuliah: {e}")
            raise

    def insert_fact_akademik(self, facts_data_list):
        if not facts_data_list:
            return
        try:
            sql_insert_fact = """
            INSERT INTO Fact_Nilai_Akademik (
                id_mahasiswa, id_waktu, id_mata_kuliah, nilai_huruf, 
                sks_mata_kuliah_earned, bobot_nilai_numeric, ip_tahap_saat_ini, 
                ipk_kumulatif_saat_ini, status_kelulusan_mk
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            extras.execute_values(self.cur, sql_insert_fact, facts_data_list)
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error inserting Fact_Nilai_Akademik: {e}")
            self.conn.rollback()
            raise
    
    def log_processed_file(self, file_name):
        try:
            self.cur.execute(
                "INSERT INTO Processed_Files_Log (file_name) VALUES (%s) ON CONFLICT (file_name) DO NOTHING;",
                (file_name,)
            )
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error logging processed file: {e}")
            self.conn.rollback()
            raise
    
    def is_file_processed(self, file_name):
        try:
            self.cur.execute("SELECT 1 FROM Processed_Files_Log WHERE file_name = %s", (file_name,))
            return self.cur.fetchone() is not None
        except psycopg2.Error as e:
            print(f"Error checking processed file log: {e}")
            raise

# --- 3. Kelas untuk Parsing PDF ---
class PdfParser:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.text = ""
        try:
            self._extract_text_from_pdf()
        except Exception as e:
            print(f"Initialization Error: Could not extract text from {pdf_path}: {e}")
            raise

    def _extract_text_from_pdf(self):
        try:
            with open(self.pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                for page_num in range(len(reader.pages)):
                    self.text += reader.pages[page_num].extract_text()
        except Exception as e:
            print(f"Error reading PDF {self.pdf_path}: {e}")
            raise

    def parse_transcript(self):
        # --- DEBUGGING: Cetak Teks Mentah dari PDF ---
        # Aktifkan ini untuk melihat teks mentah jika parsing bermasalah.
        # Kemudian komentari lagi setelah debugging selesai.
        # print("--- RAW TEXT FROM PDF ---")
        # print(self.text)
        # print("--- END RAW TEXT ---")

        data = {
            'nrp': None,
            'nama_mahasiswa': None,
            'sks_tempuh_total': None,
            'sks_lulus_total': None,
            'status_mahasiswa': None,
            'ipk_kumulatif': None,
            'grades': []
        }

        # 1. Ekstraksi Informasi Header Mahasiswa (NRP, Nama, SKS Tempuh/Lulus, Status)
        # Regex ini disesuaikan untuk format Muhammad Razan Parisya Putra_5026231174.pdf
        # Jika format transkrip lain berbeda, Anda MUNGKIN PERLU MENYESUAIKAN REGEX INI.
        header_match = re.search(
            r"NRP \/ Nama\s*\n(\d+)\s*\/\s*(.*?)\s*\nSKS Tempuh \/ SKS Lulus\s*\n(\d+)\/(\d+)\s*\nStatus\s*\n(.*?)(?:\n|$)",
            self.text, re.DOTALL
        )
        if header_match:
            data['nrp'] = header_match.group(1).strip()
            data['nama_mahasiswa'] = header_match.group(2).strip()
            data['sks_tempuh_total'] = int(header_match.group(3))
            data['sks_lulus_total'] = int(header_match.group(4))
            data['status_mahasiswa'] = header_match.group(5).strip()
        else:
            print(f"Warning: Could not extract header info from {self.pdf_path}. Skipping.")
            return None # Mengembalikan None jika header utama tidak ditemukan

        # 2. Ekstraksi IPK Kumulatif
        ipk_match = re.search(r"IPK\s*\n(\d+\.\d+)", self.text)
        if ipk_match:
            data['ipk_kumulatif'] = float(ipk_match.group(1))
        else:
            print(f"Warning: Could not extract overall IPK from {self.pdf_path}. Setting to None.")
            data['ipk_kumulatif'] = None


        # 3. Ekstraksi Nilai Mata Kuliah per Tahap
        # Ini adalah regex yang lebih kompleks untuk baris mata kuliah.
        # Hati-hati dengan karakter '\n' dan spasi di PDF.
        # Jika nama mata kuliah bisa di beberapa baris, regex perlu sangat fleksibel.
        # Pola disesuaikan untuk memecah teks menjadi bagian-bagian Tahap.
        
        # Pisahkan teks berdasarkan header Tahap
        sections = re.split(r"(Tahap: (Persiapan|Sarjana)\s*---?)", self.text)
        
        current_tahap = None
        for i in range(len(sections)):
            if sections[i].startswith("Tahap: "):
                current_tahap = sections[i].replace("Tahap: ", "").replace(" ---", "").strip()
                
                # Temukan IP untuk tahap spesifik ini
                ip_tahap = None
                ip_tahap_match = re.search(rf"IP Tahap {re.escape(current_tahap)}:\s*(\d+\.\d+)", self.text)
                if ip_tahap_match:
                    ip_tahap = float(ip_tahap_match.group(1))

                # Konten untuk bagian saat ini biasanya elemen berikutnya dalam daftar split
                section_content = sections[i+1] if (i+1) < len(sections) else ""
                
                # Hapus baris ringkasan atau header dari konten bagian
                lines = section_content.split('\n')
                relevant_lines = []
                for line in lines:
                    if not re.match(r"Total Sks Tahap (Persiapan|Sarjana):", line) and \
                       not "Kode Nama Mata Kuliah SKS Historis Nilai Nilai" in line and \
                       line.strip() != "":
                        relevant_lines.append(line)
                
                processed_text_section = "\n".join(relevant_lines)

                # Regex untuk mencocokkan setiap baris mata kuliah
                # Grup 1: Kode Mata Kuliah (misal EE234101)
                # Grup 2: Nama Mata Kuliah (bisa multi-baris)
                # Grup 3: SKS Mata Kuliah
                # Grup 4: Historis Nilai (misal 2023/Gs/A)
                # Grup 5: Nilai Akhir (misal A, AB)
                course_pattern = re.compile(
                    r"([A-Z]{2}\d{6})\s*\n?" +               # Group 1: Kode
                    r"(.+?)\s*\n?" +                         # Group 2: Nama MK (non-greedy)
                    r"(\d+)\s*\n?" +                         # Group 3: SKS
                    r"(\d{4}\/[A-Za-z]{2}\/[A-Za-z]{1,2})\s*\n?" + # Group 4: Historis Nilai
                    r"([A-Z]{1,2})\s*\n?",                   # Group 5: Nilai Akhir
                    re.DOTALL # Penting agar '.' mencocokkan newline
                )

                for match in course_pattern.finditer(processed_text_section):
                    try:
                        kode_mk = match.group(1).strip()
                        nama_mk = match.group(2).strip()
                        sks_mk = int(match.group(3))
                        historis_nilai_str = match.group(4)
                        nilai_huruf_mk = match.group(5).strip()

                        # Ekstrak tahun dan periode dari Historis Nilai
                        tahun_akademik = int(historis_nilai_str.split('/')[0])
                        periode_akademik = historis_nilai_str.split('/')[1]

                        # Validasi sederhana untuk menghindari baris header yang tak sengaja cocok
                        if "Kode" in kode_mk or "Nama Mata Kuliah" in nama_mk:
                            continue

                        data['grades'].append({
                            'tahap_akademik': current_tahap,
                            'tahun_akademik': tahun_akademik,
                            'periode_akademik': periode_akademik,
                            'kode_mata_kuliah': kode_mk,
                            'nama_mata_kuliah': nama_mk,
                            'sks_mata_kuliah_earned': sks_mk,
                            'nilai_huruf': nilai_huruf_mk,
                            'ip_tahap': ip_tahap # IP untuk tahap spesifik ini
                        })
                    except Exception as e:
                        print(f"Warning: Error parsing course line in '{current_tahap}'. Error: {e}. Raw Match: '{match.group(0)}'")
        
        return data # <<< PENTING: Mengembalikan dictionary 'data' yang sudah terisi

# --- 4. Fungsi Transformasi Tambahan (untuk measure turunan) ---
def calculate_grade_metrics(nilai_huruf):
    """Menghitung bobot nilai numerik dan status kelulusan dari nilai huruf."""
    bobot = 0.0
    lulus = False
    if nilai_huruf == 'A':
        bobot = 4.0
        lulus = True
    elif nilai_huruf == 'AB':
        bobot = 3.5
        lulus = True
    elif nilai_huruf == 'B':
        bobot = 3.0
        lulus = True
    elif nilai_huruf == 'BC':
        bobot = 2.5
        lulus = True
    elif nilai_huruf == 'C':
        bobot = 2.0
        lulus = True
    elif nilai_huruf == 'D':
        bobot = 1.0
        lulus = False
    elif nilai_huruf == 'E':
        bobot = 0.0
        lulus = False
    return bobot, lulus

# --- 5. Fungsi Orkestrasi ETL ---
def run_etl_for_transcript(pdf_path, db_manager):
    print(f"\n--- Processing {pdf_path} ---")
    try:
        # 1. Ekstraksi
        parser = PdfParser(pdf_path)
        extracted_data = parser.parse_transcript()

        if not extracted_data or extracted_data['nrp'] is None: # Periksa nrp, bukan hanya extracted_data
            print(f"Skipping {pdf_path}: Could not extract core student data (NRP missing).")
            return False

        # 2. Transformasi & Loading (dimensions first, then facts)
        
        # Load Dim_Mahasiswa (UPSERT)
        mahasiswa_id = db_manager.insert_or_get_dim_mahasiswa(
            extracted_data['nrp'],
            extracted_data['nama_mahasiswa'],
            extracted_data['sks_tempuh_total'],
            extracted_data['sks_lulus_total'],
            extracted_data['status_mahasiswa']
        )
        print(f"Processed Mahasiswa: {extracted_data['nama_mahasiswa']} (ID: {mahasiswa_id})")

        facts_to_insert = []
        for grade_info in extracted_data['grades']:
            # Load Dim_Waktu (UPSERT)
            waktu_id = db_manager.insert_or_get_dim_waktu(
                grade_info['tahun_akademik'],
                grade_info['periode_akademik'],
                grade_info['tahap_akademik']
            )

            # Load Dim_MataKuliah (UPSERT)
            matakuliah_id = db_manager.insert_or_get_dim_matakuliah(
                grade_info['kode_mata_kuliah'],
                grade_info['nama_mata_kuliah'],
                grade_info['sks_mata_kuliah_earned'] # Asumsi sks_mata_kuliah_def sama dengan sks_mata_kuliah_earned
            )

            # Hitung metrics turunan untuk Fact_Nilai_Akademik
            bobot_nilai, status_kelulusan = calculate_grade_metrics(grade_info['nilai_huruf'])

            # Siapkan data untuk Fact_Nilai_Akademik
            facts_to_insert.append((
                mahasiswa_id,
                waktu_id,
                matakuliah_id,
                grade_info['nilai_huruf'],
                grade_info['sks_mata_kuliah_earned'],
                bobot_nilai,
                grade_info['ip_tahap'],
                extracted_data['ipk_kumulatif'],
                status_kelulusan
            ))
        
        # Insert facts in batch
        db_manager.insert_fact_akademik(facts_to_insert)
        print(f"Inserted {len(facts_to_insert)} fact records for {extracted_data['nama_mahasiswa']}.")

        # Log file yang sudah diproses
        db_manager.log_processed_file(os.path.basename(pdf_path))
        print(f"Successfully processed and logged {pdf_path}.")
        return True

    except Exception as e:
        print(f"ETL process failed for {pdf_path}: {e}")
        import traceback
        traceback.print_exc() # Cetak full traceback untuk debugging
        return False

# --- 6. Fungsi untuk Beban Historis (One-Time Historical Load) ---
def historical_load(pdf_directory, db_manager):
    print("\n--- Starting Historical Load ---")
    processed_count = 0
    failed_count = 0
    
    # Kosongkan tabel log file yang diproses untuk historical load baru
    # Ini akan memastikan semua file diproses ulang jika historical_load dijalankan
    try:
        db_manager.cur.execute("TRUNCATE TABLE Processed_Files_Log RESTART IDENTITY;")
        db_manager.conn.commit()
        print("Cleared Processed_Files_Log for historical load.")
    except psycopg2.Error as e:
        print(f"Error truncating log table: {e}")
        db_manager.conn.rollback()
        # Jika truncate gagal, mungkin ada masalah koneksi atau hak akses.
        # Pertimbangkan untuk berhenti atau log lebih lanjut.

    for filename in os.listdir(pdf_directory):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(pdf_directory, filename)
            if run_etl_for_transcript(pdf_path, db_manager):
                processed_count += 1
            else:
                failed_count += 1
    print(f"\n--- Historical Load Complete ---")
    print(f"Processed: {processed_count} PDFs, Failed: {failed_count} PDFs.")

# --- 7. Fungsi untuk Beban Inkremental (Incremental Load) ---
def incremental_load(pdf_directory, db_manager):
    print("\n--- Starting Incremental Load ---")
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    for filename in os.listdir(pdf_directory):
        if filename.endswith(".pdf"):
            if not db_manager.is_file_processed(filename):
                pdf_path = os.path.join(pdf_directory, filename)
                if run_etl_for_transcript(pdf_path, db_manager):
                    processed_count += 1
                else:
                    failed_count += 1
            else:
                print(f"Skipping {filename}: Already processed.")
                skipped_count += 1
    print(f"\n--- Incremental Load Complete ---")
    print(f"Processed new: {processed_count} PDFs, Skipped (already processed): {skipped_count} PDFs, Failed: {failed_count} PDFs.")

# --- MAIN ETL PROCESS ---
if __name__ == "__main__":
    # Buat folder PDF_DIR jika belum ada
    if not os.path.exists(PDF_DIR):
        os.makedirs(PDF_DIR)
        print(f"Created directory: '{PDF_DIR}'. Please place your PDF transcripts here.")
        print("Exiting. Please place PDF files and re-run.")
        exit() 
    
    # Periksa apakah ada file PDF di folder
    if not any(f.endswith(".pdf") for f in os.listdir(PDF_DIR)):
        print(f"No PDF files found in '{PDF_DIR}'. Please place your PDF transcripts here.")
        print("Exiting. Please place PDF files and re-run.")
        exit()

    with DbManager(DB_CONFIG) as db_manager:
        if db_manager.conn and db_manager.cur: # Pastikan koneksi DB berhasil
            # --- Pilihan Mode Load ---
            # Pilih salah satu dengan mengaktifkan atau menonaktifkan baris yang sesuai
            
            # --- UNTUK BEBAN HISTORIS (Jalankan ini pertama kali) ---
            # Ini akan mencoba memproses semua file PDF di folder dan mencatatnya.
            # Jika dijalankan lagi, akan memproses ulang semua file.
            historical_load(PDF_DIR, db_manager)

            # --- UNTUK BEBAN INKREMENTAL (Jalankan ini untuk pembaruan berikutnya) ---
            # Ini hanya akan memproses file PDF yang belum ada di log.
            # incremental_load(PDF_DIR, db_manager)

        else:
            print("Failed to establish database connection. Please check DB_CONFIG.")