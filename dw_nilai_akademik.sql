DROP DATABASE IF EXISTS dw_nilai_akademik;
CREATE DATABASE dw_nilai_akademik;

CREATE TABLE dim_mahasiswa (
    id_mahasiswa SERIAL PRIMARY KEY,
    nrp VARCHAR(20) UNIQUE,
    nama_mahasiswa VARCHAR(100),
    sks_tempuh_total INT,
    sks_lulus_total INT,
    status_mahasiswa VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_matakuliah (
    id_mata_kuliah SERIAL PRIMARY KEY,
    kode_mata_kuliah VARCHAR(20) UNIQUE,
    nama_mata_kuliah VARCHAR(100),
    sks_mata_kuliah_def INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_waktu (
    id_waktu SERIAL PRIMARY KEY,
    tahun_akademik INT,
    periode_akademik VARCHAR(10),
    tahap_akademik VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tahun_akademik, periode_akademik, tahap_akademik)
);

CREATE TABLE fact_nilai_akademik (
    id SERIAL PRIMARY KEY,
    id_mahasiswa INT REFERENCES dim_mahasiswa(id_mahasiswa),
    id_waktu INT REFERENCES dim_waktu(id_waktu),
    id_mata_kuliah INT REFERENCES dim_matakuliah(id_mata_kuliah),
    nilai_huruf VARCHAR(5),
    sks_mata_kuliah_earned INT,
    bobot_nilai_numeric FLOAT,
    ip_tahap_saat_ini FLOAT,
    status_kelulusan_mk BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE fact_semester (
    id SERIAL PRIMARY KEY,
    id_mahasiswa INT REFERENCES dim_mahasiswa(id_mahasiswa),
    id_waktu INT REFERENCES dim_waktu(id_waktu),
    ip_semester FLOAT,
    ipk_kumulatif FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE processed_files_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);