# 03: Ingest telemetri mentah sampai MongoDB

**What to build:** Payload telemetri dari simulator dapat melewati MQTT dan Kafka secara end-to-end, diproses oleh Spark, dan disimpan sebagai data mentah beserta status kualitas di MongoDB.

**Blocked by:** 02 — Simulator telemetri dengan pola kampus

**Status:** done

- [x] Bridge meneruskan payload MQTT ke topic Kafka yang sesuai.
- [x] Spark dapat membaca telemetri dan menghasilkan agregat awal satu menit.
- [x] Setiap payload mentah disimpan ke MongoDB beserta metadata penerimaan.
- [x] Payload yang diterima dapat dibedakan menurut gedung, perangkat, dan jenis perangkat.
- [x] Jalur ingest dapat didemontrasikan dari simulator sampai penyimpanan mentah.
