# Spec Proyek Akhir — Jaringan Kampus (Varian B)

## Problem Statement

Kelompok membutuhkan spesifikasi implementasi yang jelas untuk proyek akhir Jaringan Kampus (Varian B) agar seluruh komponen dapat dikerjakan oleh tiga orang, memenuhi milestone, dan menghasilkan sistem end-to-end yang dapat direproduksi.

## Solution

Membangun pipeline lokal berbasis Docker Compose yang mensimulasikan 8 switch, 12 access point, dan 4 probe latensi di dua gedung.

Simulator mengirim telemetri MQTT, bridge meneruskan payload ke Kafka, Spark menghasilkan agregat dan alert streaming, MongoDB menyimpan pesan mentah, dan Airflow memuat data terbersihkan ke warehouse. Grafana menampilkan kondisi real-time dan historis, termasuk SLA, anomali trafik, perangkat offline, dan data kotor.

## User Stories

1. Sebagai tim proyek, kami ingin menyimulasikan perangkat jaringan kampus, sehingga sistem memiliki sumber data nyata secara terkontrol.
2. Sebagai pengoperasian TI, saya ingin melihat trafik setiap gedung, sehingga saya dapat membandingkan beban jaringan.
3. Sebagai pengoperasian TI, saya ingin melihat jumlah klien WiFi per gedung, sehingga saya dapat mengidentifikasi area padat.
4. Sebagai pengoperasian TI, saya ingin melihat latensi P95 setiap lima menit, sehingga saya dapat mendeteksi pelanggaran SLA.
5. Sebagai pengoperasian TI, saya ingin menerima alert saat packet loss lebih dari 2%, sehingga gangguan dapat ditangani segera.
6. Sebagai pengoperasian TI, saya ingin menerima alert saat latensi P95 lebih dari 100 ms, sehingga degradasi layanan dapat terdeteksi.
7. Sebagai pengoperasian TI, saya ingin mendeteksi trafik di luar moving-average band, sehingga lonjakan tidak biasa dapat dianalisis.
8. Sebagai pengoperasian TI, saya ingin melihat window satu menit dan lima menit, sehingga kondisi singkat dan berkelanjutan dapat dibedakan.
9. Sebagai pengoperasian TI, saya ingin menerima alert setelah perangkat tidak mengirim heartbeat selama dua menit, sehingga missing heartbeat tidak disalahartikan sebagai trafik nol.
10. Sebagai pengelola gedung, saya ingin melihat persentase kepatuhan SLA harian, sehingga kualitas layanan antar gedung dapat dibandingkan.
11. Sebagai dosen penguji, saya ingin melihat gedung paling bermasalah selama tujuh hari, sehingga bukti historis dapat didemonstrasikan.
12. Sebagai dosen penguji, saya ingin melihat tren kepatuhan SLA harian, sehingga efisiensi pemantauan dapat dievaluasi.
13. Sebagai tim proyek, kami ingin mengatur persentase counter reset, sehingga pengujian dapat direproduksi.
14. Sebagai tim proyek, kami ingin menyisipkan satuan tidak konsisten secara terkontrol, sehingga normalisasi dapat diuji.
15. Sebagai tim proyek, kami ingin menyimulasikan missing heartbeat, sehingga alert offline dapat diuji.
16. Sebagai tim proyek, kami ingin menyisipkan minimal tiga jenis data kotor, sehingga jumlah tertangkap dapat dibandingkan dengan jumlah disisipkan.
17. Sebagai engineer data, saya ingin counter reset ditandai sebagai quality event, sehingga agregat kumulatif tidak terganggu.
18. Sebagai engineer data, saya ingin counter reset dikeluarkan dari agregat kumulatif, sehingga nilai tidak negatif atau tidak realistis.
19. Sebagai engineer data, saya ingin satuan dikenali dinormalisasi, sehingga throughput memiliki unit konsisten.
20. Sebagai engineer data, saya ingin payload dengan unit ambigu ditolak, sehingga data tidak menghasilkan kesimpulan palsu.
21. Sebagai engineer data, saya ingin pesan mentah tetap tersimpan dengan status kualitas, sehingga koreksi dan audit dapat dilacak.
22. Sebagai engineer data, saya ingin master data perangkat, gedung, dan ambang SLA ada di MySQL, sehingga konfigurasi terpisah dari logika streaming.
23. Sebagai operator sistem, saya ingin pipeline dapat dijalankan melalui satu perintah Docker Compose, sehingga sistem dapat direproduksi di komputer dosen.
24. Sebagai operator sistem, saya ingin README menjelaskan langkah instalasi dan demo, sehingga evaluasi dapat dilakukan dari nol.
25. Sebagai penguji, saya ingin simulasi flash crowd dapat dipicu, sehingga anomali trafik dapat didemonstrasikan.
26. Sebagai penguji, saya ingin flash crowd menghasilkan pola trafik tinggi tetapi terkendali, sehingga tidak melanggar skala fisik simulasi.
27. Sebagai tim proyek, kami ingin memiliki minimal tujuh hari data historis, sehingga warehouse dapat menjawab tren mingguan.
28. Sebagai tim proyek, kami ingin Airflow menjalankan ETL secara terjadwal, sehingga pemuatan warehouse berjalan konsisten.
29. Sebagai tim proyek, kami ingin data quality menghasilkan jumlah disisipkan dan tertangkap per jenis, sehingga laporan dapat menyertakan bukti.
30. Sebagai anggota tim, saya ingin memiliki komponen yang terlihat pada riwayat commit Git, sehingga kontribusi individu dapat dinilai.
31. Sebagai dosen penguji, saya ingin melihat alur lengkap dari simulator sampai dashboard, sehingga demo dapat membuktikan integrasi.
32. Sebagai anggota tim, saya ingin memelihara satu komponen end-to-end, sehingga saya dapat menjawab pertanyaan teknis dan melakukan modifikasi kecil.
33. Sebagai tim proyek, kami ingin dashboard menampilkan real-time dan historis, sehingga konteks operasional dan tren tersedia bersamaan.
34. Sebagai penguji, saya ingin video demo maksimum lima menit, sehingga seluruh alur dapat ditinjau sebagai cadangan.
35. Sebagai tim proyek, kami ingin laporan mencatat arsitektur, skema, quality report, hasil signature, dan tiga temuan berbasis data.
36. Sebagai anggota tim, kami ingin mencantumkan pernyataan penggunaan AI, sehingga kepatuhan pelaporan terpenuhi.
37. Sebagai tim proyek, kami ingin file refleksi individual tersedia, sehingga pembelajaran dan kendala dapat dinilai.
38. Sebagai operator, saya ingin kegagalan komponen dapat didiagnosis melalui log, sehingga masalah demo dapat diselesaikan cepat.
39. Sebagai tim proyek, kami ingin konfigurasi threshold terpusat, sehingga perubahan lintas sistem tetap konsisten.
40. Sebagai penguji, saya ingin satu run menghasilkan hasil yang dapat dibedakan dari run sebelumnya melalui identitas simulasi yang konsisten.

## Implementation Decisions

- Sistem terdiri dari simulator Python, bridge MQTT-Kafka, Kafka, Spark Structured Streaming, MongoDB, MySQL, Airflow, ETL Pandas, warehouse, dan Grafana.
- **Gedung** adalah lokasi operasional yang mengelompokkan perangkat jaringan.
- **Switch** memublikasikan trafik port masuk, trafik port keluar, counter error, dan uptime.
- **Access point** memublikasikan jumlah klien, RSSI rata-rata, dan utilisasi kanal.
- **Probe latensi** memublikasikan latensi, packet loss, dan target pengukuran.
- Setiap payload wajib memiliki `device_id`, `device_type`, `building_id`, `timestamp`, dan `data`.
- Simulator menghasilkan minimum 24 perangkat.
- Simulator mengirim sekitar satu payload per detik untuk setiap perangkat.
- Dua gedung menjadi scope minimum warehouse dan dashboard.
- Trafik simulasi tinggi pada jam kuliah dan rendah pada malam hari.
- Flash crowd dipicu sebagai lonjakan terjadwal atau acak pada jam kuliah.
- Missing heartbeat menghasilkan status offline setelah dua menit tanpa pesan.
- Perangkat offline tidak menghasilkan metrik sintetis atau backfill trafik palsu.
- Counter reset dicatat sebagai quality event dan dikeluarkan dari agregat kumulatif.
- Satuan dikenali dinormalisasi ke Mbps untuk throughput.
- Payload dengan unit ambigu ditolak atau ditandai gagal kualitas, bukan dinormalisasi secara spekulatif.
- SLA aktif untuk window lima menit dan dihitung per gedung.
- Alert latensi muncul jika P95 latensi melebihi 100 ms dalam window lima menit.
- Alert packet loss muncul jika packet loss melebihi 2% dalam window lima menit.
- Anomali trafik dihitung menggunakan rata-rata bergerak satu jam ditambah atau dikurangi tiga standar deviasi.
- Agregat satu menit mendukung dashboard real-time.
- Agregat lima menit mendukung SLA dan alert signature.
- Data mentah MongoDB tetap menyimpan payload dan status kualitas.
- MySQL menyimpan perangkat, gedung, dan ambang konfigurasi.
- Warehouse menggunakan star schema dengan minimal satu fakta dan tiga dimensi.
- Faktanya mencakup metrik perangkat hourly, hasil SLA window, dan alert.
- Dimensinya mencakup perangkat, gedung, waktu, dan jenis alert.
- Airflow menjalankan ETL Pandas secara terjadwal dan untuk 7 hari backfill.
- Grafana menampilkan tampilan real-time dan historis.
- Sistem harus dapat dijalankan ulang dengan Docker Compose tanpa layanan cloud berbayar.

## Testing Decisions

Seam utama adalah satu jalur end-to-end menggunakan Docker Compose:

`Simulator -> MQTT -> Kafka -> Spark -> MongoDB/alert -> Airflow/ETL -> warehouse -> Grafana`

Pengujian harus memverifikasi perilaku eksternal, bukan struktur internal.

- Pengujian pengiriman memverifikasi perangkat aktif menerbitkan payload sesuai skema.
- Pengujian ingest memverifikasi MQTT diteruskan ke Kafka dan pesan mentah tersimpan di MongoDB.
- Pengujian streaming memverifikasi agregat satu menit dan lima menit muncul.
- Pengujian SLA memverifikasi P95 dan packet loss memicu kondisi yang ditentukan.
- Pengujian flash crowd memverifikasi moving-average band menandai trafik tidak normal.
- Pengujian heartbeat memverifikasi perangkat offline setelah dua menit dan alert terbentuk.
- Pengujian counter reset memverifikasi quality event tercatat dan agregat kumulatif tidak rusak.
- Pengujian satuan memverifikasi normalisasi unit dikenali dan penolakan unit ambigu.
- Pengujian ETL memverifikasi jumlah disisipkan dan tertangkap tersedia per jenis data kotor.
- Pengujian warehouse memverifikasi gedung paling bermasalah dan tren SLA selama tujuh hari dapat dijawab.
- Pengujian dashboard memverifikasi data real-time dan historis dapat ditampilkan.
- Pengujian penyebaran memverifikasi sistem dapat dijalankan ulang dari README dengan Docker Compose.

## Out of Scope

- Perangkat keras fisik.
- Sistem cloud berbayar.
- Model machine learning sebagai fitur wajib.
- Multi-kampus atau organisasi independen.
- Mobilitas perangkat antar gedung secara kompleks.
- Otentikasi enterprise, role-based access control, dan kolaborasi pengguna.
- Integrasi notifikasi SMS atau telepon.
- Protokol jaringan selain simulasi MQTT/Kafka untuk aliran utama.
- Deployment produksi atau high availability.
- Volume database yang besar atau pengujian beban tak terbatas.
- Penggunaan data nyata kampus.

## Further Notes

Pembagian peran rekomendasi untuk tiga orang:

1. Simulator, MQTT, dan Kafka.
2. Spark Structured Streaming, MongoDB, agregat, dan alert.
3. MySQL, Airflow, ETL, warehouse, Grafana, Docker Compose, serta integrasi.

Setiap anggota tetap wajib memiliki komponen yang terlihat pada riwayat commit dan mampu memodifikasi komponen miliknya saat tanya jawab.
