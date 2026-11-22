# 05: Agregat realtime dan alert SLA

**What to build:** Window streaming menghasilkan agregat jaringan satu dan lima menit, menilai SLA per gedung, serta memunculkan alert latensi atau packet loss yang dapat dilihat real-time.

**Blocked by:** 03 — Ingest telemetri mentah sampai MongoDB

**Status:** ready-for-agent

- [ ] Agregat satu menit tersedia untuk trafik port, klien WiFi, dan metrik probe.
- [ ] Window lima menit menghitung latensi P95 dan packet loss per gedung.
- [ ] Latensi P95 di atas 100 ms menghasilkan alert.
- [ ] Packet loss di atas 2% menghasilkan alert.
- [ ] Alert memuat gedung, jenis pelanggaran, timestamp, dan kondisi metrik.
