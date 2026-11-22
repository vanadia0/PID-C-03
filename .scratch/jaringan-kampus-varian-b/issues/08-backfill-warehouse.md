# 08: Backfill, ETL, dan warehouse historis

**What to build:** Airflow menjalankan ETL terjadwal untuk memuat minimal tujuh hari data historis ke warehouse star schema yang dapat menjawab gedung bermasalah dan tren SLA harian.

**Blocked by:** 01 — Fondasi reproduksibel jaringan kampus; 03 — Ingest telemetri mentah sampai MongoDB; 04 — Deteksi tiga jenis data kotor

**Status:** ready-for-agent

- [ ] MySQL menyimpan master data perangkat, gedung, dan ambang SLA.
- [ ] ETL Pandas memeriksa, menangani, dan melaporkan data quality.
- [ ] Warehouse memuat minimal satu tabel fakta dan tiga dimensi.
- [ ] Data mencakup metrik perangkat, hasil SLA, dan alert.
- [ ] Query dapat mengidentifikasi gedung paling bermasalah selama tujuh hari.
- [ ] Query dapat menampilkan tren persentase kepatuhan SLA harian per gedung.
