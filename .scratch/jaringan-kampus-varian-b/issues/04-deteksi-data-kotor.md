# 04: Deteksi tiga jenis data kotor

**What to build:** Pipeline secara terkontrol menyisipkan dan menangani counter reset, satuan tidak konsisten, dan missing heartbeat dengan laporan disisipkan-versus-tertangkap.

**Blocked by:** 03 — Ingest telemetri mentah sampai MongoDB

**Status:** ready-for-agent

- [ ] Counter reset dicatat sebagai quality event dan dikeluarkan dari agregat kumulatif.
- [ ] Satuan yang dikenali dinormalisasi ke unit standar dan unit ambigu ditolak atau ditandai gagal.
- [ ] Missing heartbeat dilaporkan sebagai quality event dan tidak menghasilkan metrik sintetis.
- [ ] Laporan data quality menyimpan jumlah payload yang disisipkan dan jumlah yang tertangkap per jenis.
- [ ] Satu run demo dapat menunjukkan ketiga jenis data kotor beserta hasil penanganannya.
