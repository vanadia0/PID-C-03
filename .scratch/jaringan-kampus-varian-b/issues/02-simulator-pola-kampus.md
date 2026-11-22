# 02: Simulator telemetri dengan pola kampus

**What to build:** Simulator menghasilkan trafik jaringan kampus yang realistis untuk semua perangkat dan mengirim telemetri ke MQTT sekitar satu kali per detik per perangkat.

**Blocked by:** 01 — Fondasi reproduksibel jaringan kampus

**Status:** done

- [x] Simulator menghasilkan 8 switch, 12 access point, dan 4 probe latensi di dua gedung.
- [x] Trafik mengikuti pola tinggi pada jam kuliah dan rendah pada malam hari.
- [x] Payload switch, access point, dan probe berisi field yang sesuai skema.
- [x] Pengaturan injeksi flash crowd dapat mengaktifkan lonjakan secara terkontrol.
- [x] Simulator dapat dikonfigurasi untuk mengirim atau menyimpan counter reset, satuan tidak konsisten, dan missing heartbeat.
