# Proyek Akhir Pengolahan dan Integrasi Data

Proyek kelompok penutup mata kuliah PID. Selama 10 minggu Anda telah mempelajari MySQL, MongoDB, ETL-Pandas, data quality, data warehouse, Airflow, batch dan stream processing, Kafka, IoT-Kafka, dan Spark. Proyek ini menggabungkan **seluruh materi tersebut** menjadi satu _pipeline_ data _end-to-end_ untuk sistem siber-fisik.

## Ketentuan Umum

- Kelompok terdiri dari **3–4 mahasiswa**.
- Terdapat **5 topik**, masing-masing dengan **Varian A dan Varian B**. Setiap kelompok mengerjakan satu varian sesuai pemetaan dosen.
- Semua data berasal dari **simulator Python buatan kelompok sendiri**. Tidak memakai perangkat keras fisik.
- Seluruh sistem berjalan **lokal dengan Docker Compose** tanpa layanan cloud berbayar dan harus dapat dijalankan ulang di komputer dosen.
- Dua kelompok pada topik yang sama mengerjakan varian berbeda dan **wajib membangun simulator serta kode masing-masing**.

## Daftar Topik

| No  | Topik                                                  | Varian A              | Varian B              |
| --- | ------------------------------------------------------ | --------------------- | --------------------- |
| 1   | [Smart Building Energy](TOPIK-1.md)                    | Gedung perkantoran    | Rumah sakit           |
| 2   | [Industrial Predictive Maintenance](TOPIK-2.md)        | Lini produksi CNC     | Stasiun pompa air     |
| 3   | [Smart Greenhouse / Precision Agriculture](TOPIK-3.md) | Greenhouse hidroponik | Irigasi lahan terbuka |
| 4   | [Fleet Telematics](TOPIK-4.md)                         | Bus kota              | Truk logistik         |
| 5   | [Data Center / Network Telemetry](TOPIK-5.md)          | Rak server            | Jaringan kampus       |

## Pemetaan Kelompok

Diisi oleh dosen.

| Topik                                 | Varian A   | Varian B   |
| ------------------------------------- | ---------- | ---------- |
| 1 — Smart Building Energy             | PID-C-\_\_ | PID-C-\_\_ |
| 2 — Industrial Predictive Maintenance | PID-C-\_\_ | PID-C-\_\_ |
| 3 — Smart Greenhouse / Agriculture    | PID-C-\_\_ | PID-C-\_\_ |
| 4 — Fleet Telematics                  | PID-C-\_\_ | PID-C-\_\_ |
| 5 — Data Center / Network Telemetry   | PID-C-\_\_ | PID-C-\_\_ |

## Core Stack (Wajib untuk Semua Topik)

```text
Simulator Python ──MQTT──▶ Bridge ──▶ Kafka ──▶ Spark Structured Streaming ──▶ Alert + agregat real-time
                                         │                                          │
                                         ▼                                          ▼
                                   MongoDB (raw)                              Dashboard
                                         │                                 (Grafana / Flask)
                                         ▼                                          ▲
  MySQL (master data) ──▶ Airflow DAG: ETL Pandas + data quality ──▶ Data warehouse (star schema)
```

| Komponen          | Ketentuan                                                             | Modul rujukan                    |
| ----------------- | --------------------------------------------------------------------- | -------------------------------- |
| Simulator         | Python, publish JSON ke MQTT, menyisipkan data kotor                  | `iot-kafka/`                     |
| Ingestion         | MQTT → Kafka (bridge)                                                 | `iot-kafka/`, `stream/`          |
| Stream processing | Spark Structured Streaming: agregasi _windowed_ dan alert real-time   | `spark/`, `batch-stream/`        |
| Raw storage       | MongoDB menyimpan pesan mentah                                        | `mongo/`                         |
| Master data       | MySQL menyimpan data referensi (perangkat, lokasi, ambang batas)      | `Mysql/`                         |
| Batch ETL         | Pandas dengan pemeriksaan dan penanganan data quality                 | `ETL-Pandas/`, `data-quality/`   |
| Orkestrasi        | Airflow DAG terjadwal untuk ETL dan pemuatan warehouse                | `airflow/`                       |
| Data warehouse    | Star schema (minimal 1 tabel fakta, 3 dimensi), PostgreSQL atau MySQL | `Load-data ke warehouse/`        |
| Dashboard         | Grafana atau Flask, menampilkan tampilan real-time **dan** historis   | `iot-kafka/`, `spark/`, `steam/` |

## Skala Minimum

- Minimal **20 perangkat simulasi**.
- Laju sekitar **1 pesan per detik per perangkat**.
- Minimal **7 hari data historis** hasil _backfill_ untuk warehouse dan DAG.
- Minimal **3 jenis data kotor** yang disisipkan simulator secara sengaja dan terkontrol (persentase dapat diatur), dipilih dari:
  - nilai hilang (_missing_/`null`)
  - pesan duplikat
  - nilai di luar rentang fisik (_out-of-range_)
  - pesan terlambat atau tidak berurutan (_late/out-of-order_)
  - format tidak konsisten (satuan, huruf besar-kecil, format waktu)

Pipeline wajib **mendeteksi, melaporkan, dan menangani** setiap jenis data kotor tersebut. Laporan harus menyebutkan berapa yang disisipkan dan berapa yang tertangkap.

## Tantangan Signature

Setiap varian memiliki satu **tantangan signature** khas domain (lihat berkas topik). Tantangan ini dikerjakan dengan metode **berbasis aturan atau statistik** (ambang batas, z-score, rata-rata bergerak, tren linear). Model machine learning bersifat **opsional sebagai bonus**.

## Milestone

Setiap milestone diperiksa dengan daftar _pass/fail_.

| Minggu | Milestone    | Item yang diperiksa                                                                                                                                          |
| ------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| W1     | Proposal     | 1. Diagram arsitektur · 2. Daftar perangkat dan skema pesan · 3. Pembagian peran · 4. Varian yang dikerjakan                                                 |
| W3     | Checkpoint 1 | 1. Simulator publish ke MQTT → Kafka · 2. Spark streaming menghasilkan agregat _windowed_ · 3. Data mentah masuk MongoDB                                     |
| W5     | Checkpoint 2 | 1. Airflow DAG menjalankan ETL dengan data quality · 2. Warehouse star schema terisi · 3. Dashboard real-time dan historis · 4. Tantangan signature berjalan |
| W6     | Final        | 1. Demo penuh · 2. Laporan · 3. Slide dan video demo · 4. Refleksi individu                                                                                  |

## Pembagian Peran (Disarankan)

| Peran                        | Tanggung jawab utama                               |
| ---------------------------- | -------------------------------------------------- |
| Ingestion & Streaming        | Simulator, MQTT, Kafka, Spark Structured Streaming |
| Storage & Warehouse          | MongoDB, MySQL, desain dan pemuatan star schema    |
| Orchestration & Data Quality | Airflow DAG, ETL Pandas, pemeriksaan data quality  |
| Dashboard & Integration      | Dashboard, Docker Compose, integrasi end-to-end    |

Untuk kelompok 3 orang, dua peran boleh digabung. Peran bersifat saran, tetapi **setiap anggota wajib memiliki minimal satu komponen** yang terlihat dari riwayat commit Git.

## Berkas yang Dikumpulkan

```text
PID-C-XX/
├── docker-compose.yml       # sistem berjalan dengan `docker compose up`
├── README.md                # langkah menjalankan dari nol
├── laporan.md               # arsitektur, skema, temuan data quality, analisis
├── slides.pdf
├── video-demo.(mp4|link)    # maksimal 5 menit, cadangan bila demo langsung gagal
├── refleksi/                # satu berkas per anggota: NIM_Nama.md
└── src/ ...                 # simulator, bridge, spark, dags, dashboard, sql
```

`laporan.md` minimal memuat: diagram arsitektur, skema pesan, star schema, ringkasan data quality (disisipkan vs. tertangkap), hasil tantangan signature, tiga temuan berbasis data, dan pernyataan penggunaan AI.

Jangan mengumpulkan volume database, virtual environment, atau kata sandi pribadi. Gunakan `.env.example` untuk kredensial.

## Kebijakan AI Generatif

AI generatif **boleh digunakan dengan pengungkapan**. Cantumkan di `laporan.md` bagian mana yang dibantu AI dan alat apa yang dipakai. Setiap anggota harus mampu menjelaskan komponen miliknya saat sesi tanya jawab; ketidakmampuan menjelaskan memengaruhi nilai kontribusi individu.

## Penilaian

Lihat [RUBRIK.md](RUBRIK.md).
