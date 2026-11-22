# 09: Dashboard dan demo end-to-end

**What to build:** Grafana menampilkan data real-time dan historis dalam satu demonstrasi dari simulator sampai warehouse melalui Docker Compose.

**Blocked by:** 05 — Agregat realtime dan alert SLA; 07 — Anomali trafik moving-average; 08 — Backfill, ETL, dan warehouse historis

**Status:** ready-for-agent

- [ ] Dashboard menampilkan trafik, klien WiFi, latensi P95, packet loss, dan status perangkat.
- [ ] Dashboard menampilkan alert SLA, heartbeat offline, dan anomali trafik.
- [ ] Dashboard menampilkan laporan SLA harian dan gedung paling bermasalah.
- [ ] Tampilan real-time dan historis dapat dibedakan namun digunakan dalam satu dashboard.
- [ ] Satu `docker compose up` memulai alur yang dapat dijalankan untuk demo.
