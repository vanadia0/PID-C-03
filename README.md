# PID-C-XX - Storage, Warehouse, dan Integrasi

Implementasi bagian C untuk Varian B: Jaringan Kampus.

## Komponen

- MySQL: master data gedung, perangkat, dan threshold.
- MongoDB: penyimpanan payload mentah dan status kualitas.
- ETL Pandas: normalisasi unit, quality report, dan backfill tujuh hari.
- PostgreSQL: star schema dan fakta data warehouse.
- Airflow: menjadwalkan ETL setiap jam.
- Flask: dashboard operasional dan API.
- Grafana: datasource warehouse siap digunakan.
- Docker Compose: penyebaran satu perintah.

## Jalankan dari Nol

1. Masuk ke folder proyek:

```bash
cd "Projek PID/PID-C-XX"
```

2. Jalankan seluruh sistem:

```bash
docker compose up -d --build
```

3. Tunggu container sehat, lalu jalankan ETL dari dashboard:

- Dashboard: http://localhost:5000
- Airflow: http://localhost:8080
- Grafana: http://localhost:3000

Di VS Code, buka tab **Ports** dan pastikan port berikut berstatus forwarded:

- `5000` — Dashboard
- `3000` — Grafana
- `8080` — Airflow
- `27018` — MongoDB proyek

Jika browser menampilkan **Failed to load page**, buka alamat dari tab Ports, bukan
alamat yang diketik secara manual. Pilih port `5000`, lalu klik **Open in Browser**.

Kredensial lokal:

- Dashboard tidak membutuhkan login.
- Grafana: `admin` / `admin`
- Airflow: login dibuat Airflow standalone saat pertama kali dijalankan.

Grafana otomatis menyediakan dashboard **Network Campus Telemetry**. Pilih
rentang waktu **Last 7 days** untuk melihat metrik warehouse.

Payload jaringan dari simulator/streaming dapat disimpan ke MongoDB collection
`network_telemetry.raw_network_metrics` dengan field `device_id`, `device_type`,
`building_id`, `timestamp`, dan `data`. Dashboard juga menyediakan endpoint
`POST /api/ingest` untuk mengirim payload ke collection tersebut. ETL membaca
payload non-demo terlebih dahulu; jika belum ada, ia memakai data demo tujuh hari.

Hasil signature dari Spark dapat dikirim ke `POST /api/stream/alerts` agar ikut
masuk ke warehouse dan panel Grafana:

```bash
curl -X POST http://localhost:5000/api/stream/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": "2026-11-24T10:05:00Z",
    "building_id": "GEDUNG-B",
    "device_id": "PROBE-03",
    "alert_code": "TRAFFIC_ANOMALY",
    "severity": "high",
    "message": "Traffic di atas moving-average band"
  }'
```

Setelah itu jalankan DAG `network_warehouse_etl` untuk memuat alert ke warehouse.

ETL normalisasi `port_in_mbps`/`port_out_mbps`, latensi, packet loss, utilisasi
kanal, dan RSSI. Throughput masuk/keluar diringkas sebagai total trafik Mbps
untuk fakta hourly. Payload dengan field lokasi atau waktu invalid ditolak dari
warehouse dan errornya ditampilkan di task Airflow.

Atau jalankan ETL langsung dari host jika Python sudah memasang dependency:

```bash
python src/etl/etl_network.py
```

## Alur Demo

1. Buka dashboard dan tekan **Jalankan ETL 7 Hari**.
2. Tunjukkan tabel gedung paling bermasalah.
3. Tunjukkan tren kepatuhan SLA harian.
4. Tunjukkan quality report:
   - unit dinormalisasi;
   - unit ambigu ditolak;
   - counter reset ditandai.
5. Buka `/api/alerts` untuk melihat alert latensi dan packet loss.
6. Kirim satu payload uji:

```bash
curl -X POST http://localhost:5000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "SW-99",
    "device_type": "switch",
    "building_id": "GEDUNG-A",
    "timestamp": "2026-11-24T10:00:00Z",
    "data": {"traffic_mbps": "500mbps"}
  }'
```

## Pengujian ETL

Dependency:

```bash
python -m pip install pandas psycopg2-binary pymongo Flask pytest
python -m pytest tests/test_etl_network.py -q
```

## Query Bukti

Gedung paling bermasalah:

```sql
SELECT b.building_name,
       COUNT(a.alert_id) AS alert_count
FROM fact_alert a
JOIN dim_building b ON b.building_key = a.building_key
GROUP BY b.building_name
ORDER BY alert_count DESC;
```

Tren SLA harian:

```sql
SELECT t.date_value,
       b.building_name,
       AVG(f.sla_compliance_pct) AS compliance_pct
FROM fact_sla_window f
JOIN dim_building b ON b.building_key = f.building_key
JOIN dim_time t ON t.time_key = f.time_key
GROUP BY t.date_value, b.building_name
ORDER BY t.date_value, b.building_name;
```

Kualitas:

```sql
SELECT issue_type, SUM(inserted_count), SUM(caught_count)
FROM fact_data_quality
GROUP BY issue_type;
```

## Penghentian

```bash
docker compose down
```

Untuk menghapus seluruh data demonstrasi:

```bash
docker compose down -v
```
