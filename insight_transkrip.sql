-- Insight 1: Rata-rata IPK seluruh mahasiswa
SELECT
    AVG(total_bobot / total_sks) AS rata_rata_ipk
FROM (
    SELECT m.id_mahasiswa,
           SUM(n.bobot * mk.sks) AS total_bobot,
           SUM(mk.sks) AS total_sks
    FROM Fact_Nilai_MK f
    JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
    JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
    JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
    GROUP BY m.id_mahasiswa
) AS ipk_mahasiswa;

-- Insight 2: Top 10 Mahasiswa Berdasarkan IPK
SELECT
    m.nrp, m.nama,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS ipk
FROM Fact_Nilai_MK f
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
JOIN Fact_Nilai_MK f ON f.id_nilai = n.id_nilai
GROUP BY huruf
ORDER BY jumlah DESC;

-- Insight 4: Top 5 MK nilai rata-rata terendah
SELECT
    mk.kode_mk, mk.nama_mk,
    ROUND(AVG(n.bobot), 2) AS rata_rata_bobot
FROM Fact_Nilai_MK f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY mk.id_mk
ORDER BY rata_rata_bobot ASC
LIMIT 5;

-- Insight 5: Top 5 MK nilai rata-rata tertinggi
SELECT
    mk.kode_mk, mk.nama_mk,
    ROUND(AVG(n.bobot), 2) AS rata_rata_bobot
FROM Fact_Nilai_MK f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY mk.id_mk
ORDER BY rata_rata_bobot DESC
LIMIT 5;

-- Insight 6: Nilai rata-rata semester gasal vs genap
SELECT
    w.semester,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS rata_rata_nilai
FROM Fact_Nilai_MK f
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY w.semester;

-- Insight 7: Rata-rata IPS per semester
SELECT
    w.tahun, w.semester,
    ROUND(SUM(n.bobot * mk.sks) / SUM(mk.sks), 2) AS rata_ips
FROM Fact_Nilai_MK f
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY w.tahun, w.semester
ORDER BY w.tahun, w.semester;

-- Insight 8: Mata kuliah paling sering diambil
SELECT
    mk.kode_mk, mk.nama_mk,
    COUNT(*) AS frekuensi_pengambilan
FROM Fact_Nilai_MK f
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
GROUP BY mk.id_mk
ORDER BY frekuensi_pengambilan DESC
LIMIT 5;

-- Insight 9: Total SKS lulus per mahasiswa
SELECT
    m.nrp, m.nama,
    SUM(mk.sks) AS total_sks_lulus
FROM Fact_Nilai_MK f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f.id_mk = mk.id_mk
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
WHERE n.bobot >= 2.0
GROUP BY m.id_mahasiswa;

-- Insight 10: Mahasiswa belum lulus suatu MK (nilai D/E tanpa perbaikan)
SELECT DISTINCT
    m.nrp, m.nama,
    mk.kode_mk, mk.nama_mk
FROM Fact_Nilai_MK f1
JOIN Dim_Mahasiswa m ON f1.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON f1.id_mk = mk.id_mk
JOIN Dim_Nilai n1 ON f1.id_nilai = n1.id_nilai
WHERE n1.huruf IN ('D', 'E')
  AND NOT EXISTS (
    SELECT 1 FROM Fact_Nilai_MK f2
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
FROM Fact_Nilai_MK f
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
FROM Fact_Nilai_MK f
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
FROM Fact_Nilai_MK f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY m.id_mahasiswa
ORDER BY rata_rata_bobot_nilai DESC;

-- Insight 19: Jumlah semester yang diikuti setiap mahasiswa
SELECT
    m.nrp,
    m.nama,
    COUNT(DISTINCT CONCAT(w.tahun, '-', w.semester)) AS jumlah_semester
FROM Fact_Nilai_MK f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
GROUP BY m.id_mahasiswa
ORDER BY jumlah_semester DESC;

-- Insight 20: Nilai terendah yang pernah dicapai tiap mahasiswa
SELECT
    m.nrp,
    m.nama,
    MIN(n.bobot) AS nilai_terendah
FROM Fact_Nilai_MK f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Nilai n ON f.id_nilai = n.id_nilai
GROUP BY m.id_mahasiswa
ORDER BY nilai_terendah ASC;

-- Insight 21  : Tren IPS per Mahasiswa
SELECT
    m.nrp,
    w.tahun,
    w.semester,
    f.ips
FROM Fact_Nilai_Semester f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
ORDER BY m.nrp, w.tahun, w.semester;

-- Insight 22  : Mahasiswa dengan Kenaikan (DESC)/Penurunan(ASC) IPS Terbesar Antar Semester
SELECT
    nrp,
    nama,
    semester_sebelumnya,
    semester_sekarang,
    ROUND(kenaikan_ips, 2) AS selisih_ips
FROM (
    SELECT
        m.nrp,
        m.nama,
        CONCAT(w.tahun, '-', w.semester) AS semester_sekarang,
        CONCAT(LAG(w.tahun) OVER (PARTITION BY m.id_mahasiswa ORDER BY w.tahun, w.semester),
               '-', 
               LAG(w.semester) OVER (PARTITION BY m.id_mahasiswa ORDER BY w.tahun, w.semester)
        ) AS semester_sebelumnya,
        f.ips - LAG(f.ips) OVER (PARTITION BY m.id_mahasiswa ORDER BY w.tahun, w.semester) AS kenaikan_ips
    FROM Fact_Nilai_Semester f
    JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
    JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
) AS perubahan
WHERE kenaikan_ips IS NOT NULL
ORDER BY selisih_ips DESC
LIMIT 10;


-- Insight 23  : Mahasiswa dengan IPK Stabil Tinggi
SELECT
    m.nrp, m.nama,
    MIN(f.ipk) AS ipk_terendah,
    MAX(f.ipk) AS ipk_tertinggi
FROM Fact_Nilai_Semester f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
GROUP BY m.id_mahasiswa
HAVING MIN(f.ipk) >= 3.5;

-- Insight 24  : Rata-rata IPK seluruh mahasiswa semester akhir dibanding awal
SELECT
    ROUND(AVG(ipk_akhir), 2) AS rata_ipk_akhir,
    ROUND(AVG(ipk_awal), 2) AS rata_ipk_awal
FROM (
    SELECT
        f.id_mahasiswa,
        FIRST_VALUE(f.ipk) OVER (PARTITION BY f.id_mahasiswa ORDER BY w.tahun, w.semester) AS ipk_awal,
        LAST_VALUE(f.ipk) OVER (PARTITION BY f.id_mahasiswa ORDER BY w.tahun, w.semester
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ipk_akhir
    FROM Fact_Nilai_Semester f
    JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
) AS ipk_window;


-- Insight 25  : Jumlah mahasiswa dengan IPK < 2.0 (bisa dirubah) di semester akhir
SELECT
    COUNT(*) AS jumlah_mahasiswa_ipk_buruk
FROM (
    SELECT f.id_mahasiswa,
           f.ipk,
           w.tahun, w.semester,
           RANK() OVER (PARTITION BY f.id_mahasiswa ORDER BY w.tahun DESC, w.semester DESC) AS urutan
    FROM Fact_Nilai_Semester f
    JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
) AS terakhir
WHERE urutan = 1 AND ipk < 2.0;

-- Insight 26  : Distribusi IPS antar semester (boxplot-ready)
SELECT
    w.tahun,
    w.semester,
    f.ips
FROM Fact_Nilai_Semester f
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu;

-- Insight 27  : Korelasi antara IPS saat ini dan sebelumnya
SELECT
    f1.id_mahasiswa,
    w1.tahun AS tahun_sekarang,
    w1.semester AS semester_sekarang,
    f1.ips AS ips_sekarang,
    f2.ips AS ips_sebelumnya
FROM Fact_Nilai_Semester f1
JOIN Dim_Waktu w1 ON f1.id_waktu = w1.id_waktu
JOIN Fact_Nilai_Semester f2 ON f2.id_mahasiswa = f1.id_mahasiswa
JOIN Dim_Waktu w2 ON f2.id_waktu = w2.id_waktu
WHERE (w1.tahun > w2.tahun) OR (w1.tahun = w2.tahun AND w1.semester = 'Genap' AND w2.semester = 'Gasal')
  AND NOT EXISTS (
      SELECT 1 FROM Fact_Nilai_Semester f3
      JOIN Dim_Waktu w3 ON f3.id_waktu = w3.id_waktu
      WHERE f3.id_mahasiswa = f1.id_mahasiswa
        AND ((w3.tahun > w2.tahun) OR (w3.tahun = w2.tahun AND w3.semester = 'Genap'))
        AND ((w3.tahun < w1.tahun) OR (w3.tahun = w1.tahun AND w3.semester = 'Gasal'))
  );

-- Insight 28  : Ranking mahasiswa per semester berdasarkan IPS
SELECT
    w.tahun,
    w.semester,
    m.nrp,
    m.nama,
    f.ips,
    RANK() OVER (PARTITION BY w.tahun, w.semester ORDER BY f.ips DESC) AS peringkat_ips
FROM Fact_Nilai_Semester f
JOIN Dim_Mahasiswa m ON f.id_mahasiswa = m.id_mahasiswa
JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
ORDER BY w.tahun, w.semester, peringkat_ips;

-- Insight 29  : Identifikasi semester dengan penurunan kolektif IPS terbesar
SELECT
    prev_semester,
    curr_semester,
    ROUND(prev_avg, 2) AS rata_ips_sebelumnya,
    ROUND(curr_avg, 2) AS rata_ips_sekarang,
    ROUND(prev_avg - curr_avg, 2) AS penurunan
FROM (
    SELECT
        CONCAT(w.tahun, '-', w.semester) AS curr_semester,
        LAG(CONCAT(w.tahun, '-', w.semester)) OVER (ORDER BY w.tahun, w.semester) AS prev_semester,
        AVG(f.ips) AS curr_avg,
        LAG(AVG(f.ips)) OVER (ORDER BY w.tahun, w.semester) AS prev_avg
    FROM Fact_Nilai_Semester f
    JOIN Dim_Waktu w ON f.id_waktu = w.id_waktu
    GROUP BY w.tahun, w.semester
) AS delta
WHERE prev_avg IS NOT NULL
ORDER BY penurunan DESC
LIMIT 1;

-- Insight 30  : 