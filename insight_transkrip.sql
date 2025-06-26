-- Insight 1: Rata-rata IPK seluruh mahasiswa
SELECT
    AVG(total_bobot / total_sks) AS rata_rata_ipk
FROM (
    SELECT m.id_mahasiswa,
           SUM(n.bobot * mk.sks) AS total_bobot,
           SUM(mk.sks) AS total_sks
    FROM Fact_Transkrip f
    JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
    JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
    JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
    GROUP BY m.id_mahasiswa
) AS ipk_mahasiswa;

-- Insight 2: Top 10 Mahasiswa Berdasarkan IPK
SELECT
    m.nrp, m.nama,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS ipk
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY m.id_mahasiswa
ORDER BY ipk DESC
LIMIT 10;

-- Insight 3: Distribusi nilai huruf
SELECT
    huruf,
    COUNT(*) AS jumlah
FROM Dim_Nilai n
JOIN Fact_Transkrip f ON f.id_nilai = n.id_nilai
GROUP BY huruf
ORDER BY jumlah DESC;

-- Insight 4: Top 5 MK nilai rata-rata terendah
SELECT
    mk.kode_mk, mk.nama_mk,
    ROUND(AVG(n.bobot), 2) AS rata_rata_bobot
FROM Fact_Transkrip f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY mk.id_mk
ORDER BY rata_rata_bobot ASC
LIMIT 5;

-- Insight 5: Top 5 MK nilai rata-rata tertinggi
SELECT
    mk.kode_mk, mk.nama_mk,
    ROUND(AVG(n.bobot), 2) AS rata_rata_bobot
FROM Fact_Transkrip f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY mk.id_mk
ORDER BY rata_rata_bobot DESC
LIMIT 5;

-- Insight 6: Nilai rata-rata semester gasal vs genap
SELECT
    w.semester,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS rata_rata_nilai
FROM Fact_Transkrip f
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY w.semester;

-- Insight 7: Rata-rata IPS per semester
SELECT
    w.tahun, w.semester,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS rata_ips
FROM Fact_Transkrip f
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY w.tahun, w.semester
ORDER BY w.tahun, w.semester;

-- Insight 8: Mata kuliah paling sering diambil
SELECT
    mk.kode_mk, mk.nama_mk,
    COUNT(*) AS frekuensi_pengambilan
FROM Fact_Transkrip f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
GROUP BY mk.id_mk
ORDER BY frekuensi_pengambilan DESC
LIMIT 5;

-- Insight 9: Total SKS lulus per mahasiswa
SELECT
    m.nrp, m.nama,
    SUM(mk.sks) AS total_sks_lulus
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
WHERE n.bobot >= 2.0
GROUP BY m.id_mahasiswa;

-- Insight 10: Mahasiswa belum lulus suatu MK (nilai D/E tanpa perbaikan)
SELECT DISTINCT
    m.nrp, m.nama,
    mk.kode_mk, mk.nama_mk
FROM Fact_Transkrip f1
JOIN Dim_Mahasiswa m ON f1.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f1.id_mk = mk.id_mk
JOIN Dim_Nilai n1 ON f1.id_nilai = n1.id_nilai
WHERE n1.huruf IN ('D', 'E')
  AND NOT EXISTS (
    SELECT 1 FROM Fact_Transkrip f2
    JOIN Dim_Nilai n2 ON f2.id_nilai = n2.id_nilai
    WHERE f2.id_mahasiswa = f1.id_mahasiswa
      AND f2.id_mk = f1.id_mk
      AND n2.huruf NOT IN ('D', 'E')
  );

-- Insight 11: Mahasiswa yang mengulang MK (ambil MK lebih dari 1x)
SELECT
    m.nrp, m.nama,
    mk.kode_mk, mk.nama_mk,
    COUNT(*) AS kali_diambil
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
GROUP BY m.id_mahasiswa, mk.id_mk
HAVING COUNT(*) > 1
ORDER BY kali_diambil DESC;

-- Insight 12: Rata-rata IP tahap persiapan vs sarjana
SELECT
    ROUND(AVG(ip_persiapan), 2) AS rata_persiapan,
    ROUND(AVG(ip_sarjana), 2) AS rata_sarjana
FROM Dim_Mahasiswa
WHERE ip_persiapan > 0 AND ip_sarjana > 0;

-- Insight 13: Top 5 IP tahap persiapan tertinggi
SELECT nrp, nama, ip_persiapan
FROM Dim_Mahasiswa
ORDER BY ip_persiapan DESC
LIMIT 5;

-- Insight 14: Top 5 IP tahap sarjana tertinggi
SELECT nrp, nama, ip_sarjana
FROM Dim_Mahasiswa
ORDER BY ip_sarjana DESC
LIMIT 5;

-- Insight 15: Kelulusan Mahasiswa per Mata Kuliah
SELECT
    mk.kode_mk, mk.nama_mk,
    SUM(CASE WHEN n.huruf NOT IN ('D','E') THEN 1 ELSE 0 END) AS lulus,
    SUM(CASE WHEN n.huruf IN ('D','E') THEN 1 ELSE 0 END) AS tidak_lulus
FROM Fact_Transkrip f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY mk.id_mk;

-- Insight 16: Jalur masuk mahasiswa (berdasarkan NRP)
SELECT
    nrp, nama,
    CASE
        WHEN SUBSTRING(nrp, 8, 3) BETWEEN '001' AND '042' THEN 'SNBP'
        WHEN SUBSTRING(nrp, 8, 3) BETWEEN '043' AND '116' THEN 'SNBT'
        WHEN SUBSTRING(nrp, 8, 3) BETWEEN '117' AND '232' THEN 'Mandiri'
        ELSE 'Lainnya'
    END AS jalur_masuk
FROM Dim_Mahasiswa;

-- Insight 17: Rata-rata IPK per jalur masuk
SELECT
    jalur_masuk,
    ROUND(AVG(ipk),2) AS rata_rata_ipk,
    COUNT(*) AS jumlah_mahasiswa
FROM (
    SELECT
        nrp, ipk,
        CASE
            WHEN SUBSTRING(nrp, 8, 3) BETWEEN '001' AND '042' THEN 'SNBP'
            WHEN SUBSTRING(nrp, 8, 3) BETWEEN '043' AND '116' THEN 'SNBT'
            WHEN SUBSTRING(nrp, 8, 3) BETWEEN '117' AND '232' THEN 'Mandiri'
            ELSE 'Lainnya'
        END AS jalur_masuk
    FROM Dim_Mahasiswa
) AS m
GROUP BY jalur_masuk
ORDER BY rata_rata_ipk DESC;

-- Insight 18: Rata-rata bobot nilai per mahasiswa (tanpa pembobotan SKS)
SELECT
    m.nrp,
    m.nama,
    ROUND(AVG(n.bobot), 2) AS rata_rata_bobot_nilai
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY m.id_mahasiswa
ORDER BY rata_rata_bobot_nilai DESC;

-- Insight 19: Jumlah semester yang diikuti setiap mahasiswa
SELECT
    m.nrp,
    m.nama,
    COUNT(DISTINCT CONCAT(w.tahun, '-', w.semester)) AS jumlah_semester
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
GROUP BY m.id_mahasiswa
ORDER BY jumlah_semester DESC;

-- Insight 20: Nilai terendah yang pernah dicapai tiap mahasiswa
SELECT
    m.nrp,
    m.nama,
    MIN(n.bobot) AS nilai_terendah
FROM Fact_Transkrip f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY m.id_mahasiswa
ORDER BY nilai_terendah ASC;

