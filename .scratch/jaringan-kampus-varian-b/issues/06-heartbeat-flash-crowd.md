# 06: Heartbeat offline dan flash crowd

**What to build:** Sistem mendeteksi perangkat yang berhenti mengirim heartbeat setelah dua menit dan merepresentasikan flash crowd sebagai lonjakan terjadwal yang dapat dijadikan bukti demo.

**Blocked by:** 03 — Ingest telemetri mentah sampai MongoDB; 04 — Deteksi tiga jenis data kotor

**Status:** ready-for-agent

- [ ] Perangkat yang tidak mengirim pesan selama lebih dari dua menit berstatus offline dan memunculkan alert.
- [ ] Status offline tidak mengisi metrik nyata dengan nilai sintetis.
- [ ] Flash crowd dapat dipicu melalui konfigurasi untuk satu periode pada jam kuliah.
- [ ] Periode flash crowd tercatat dan dapat dibedakan dari trafik normal.
- [ ] Dashboard dan log dapat menunjukkan transisi normal, flash crowd, dan offline.
