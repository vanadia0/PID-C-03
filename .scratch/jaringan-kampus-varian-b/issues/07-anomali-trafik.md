# 07: Anomali trafik moving-average

**What to build:** Sistem menandai trafik yang berada di luar rata-rata bergerak satu jam ditambah atau dikurangi tiga standar deviasi.

**Blocked by:** 03 — Ingest telemetri mentah sampai MongoDB; 06 — Heartbeat offline dan flash crowd

**Status:** ready-for-agent

- [ ] Perhitungan menggunakan agregat trafik port yang konsisten dan window satu jam.
- [ ] Nilai di luar moving-average band ditandai sebagai anomali.
- [ ] Anomali memuat gedung, perangkat, timestamp, nilai aktual, dan batas band.
- [ ] Flash crowd yang melewati band dapat dibuktikan sebagai anomali, bukan sekadar trafik tinggi normal.
- [ ] Hasil anomali dapat ditampilkan real-time dan disimpan untuk analisis historis.
