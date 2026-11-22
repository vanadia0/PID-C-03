origin  https://github.com/vanadia0/PID-C-03 (fetch)
origin  https://github.com/vanadia0/PID-C-03 (push)# PID-C-03 — Jaringan Kampus

Fondasi lokal untuk pipeline telemetri Jaringan Kampus Varian B. Semua layanan
berjalan di komputer lokal melalui Docker Compose; tidak ada layanan cloud.

## Prasyarat

- Docker Desktop atau Docker Engine dengan plugin Docker Compose v2.
- Minimal 8 GB RAM yang dialokasikan ke Docker untuk menjalankan seluruh stack.

Di Windows, instal Docker Desktop dari
[panduan resmi Docker](https://docs.docker.com/desktop/setup/install/windows-install/),
lalu pastikan perintah `docker --version` dan `docker compose version` tersedia.

## Menyalakan lingkungan

Dari direktori root proyek (`PID-C-03`), salin konfigurasi contoh dan nyalakan
layanan:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up -d
docker compose ps
```

`docker compose config` memeriksa konfigurasi sebelum layanan dijalankan.
Compose mengunduh image yang diperlukan saat pertama kali dijalankan. Untuk
melihat kata sandi admin Airflow yang dibuat saat inisialisasi, jalankan
`docker compose logs airflow`; nama penggunanya `admin`.

| Layanan | Alamat lokal |
| --- | --- |
| Grafana | <http://localhost:3000> |
| Airflow | <http://localhost:8080> |
| Spark master UI | <http://localhost:8081> |
| MQTT | `localhost:1883` |
| Kafka (client host) | `localhost:29092` |
| MongoDB | `mongodb://localhost:27017` |
| MySQL | `localhost:3306` |
| PostgreSQL warehouse | `localhost:5432` |

Semua port dipetakan ke loopback agar layanan fondasi tidak terbuka ke jaringan
lokal. Nilai kredensial di `.env.example` hanya untuk pengembangan lokal, bukan
untuk deployment produksi. Broker MQTT mengizinkan koneksi anonim; jangan
mengubah binding port menjadi terbuka ke jaringan tanpa menambahkan autentikasi.

Daftar gedung dan perangkat ada di [`config/campus.json`](config/campus.json).
Kontrak payload berada di [`schemas/telemetry.schema.json`](schemas/telemetry.schema.json).
Direktori `airflow/dags` adalah lokasi DAG proyek.

## Simulator telemetri

Simulator dijalankan otomatis sebagai layanan `simulator`. Untuk mengubah
pengujian dapat diatur sebelum `docker compose up -d` di `.env`:

```dotenv
INTERVAL_SECONDS=1
FLASH_CROWD=false
COUNTER_RESET_PROBABILITY=0.01
INCONSISTENT_UNIT_PROBABILITY=0.01
MISSING_HEARTBEAT_PROBABILITY=0.001
```

Nilai probabilitas berkisar dari `0` sampai `1`. Untuk memicu lonjakan secara
terkendali, gunakan `FLASH_CROWD=true`. Lihat telemetri melalui MQTT dengan:

```powershell
docker compose exec mqtt mosquitto_sub -h localhost -t "campus/telemetry/#" -v
```

## Jalur ingest

Setelah simulator berjalan, MQTT diteruskan ke Kafka oleh
`mqtt-kafka-bridge`, lalu diproses oleh `spark-ingest` dan disimpan ke MongoDB:

```powershell
docker compose up -d mqtt kafka mongodb mqtt-kafka-bridge spark-ingest simulator
docker compose ps
docker compose logs --tail=20 spark-ingest mqtt-kafka-bridge
```

Dokumen mentah tersedia pada MongoDB database `network`, koleksi
`telemetry_raw`. Contoh pemeriksaan:

```powershell
docker compose exec mongodb mongosh network --quiet --eval "db.telemetry_raw.find().limit(3).forEach(printjson)"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic campus.telemetry --from-beginning --max-messages 3
```

## Menghentikan lingkungan

Hentikan container tetapi pertahankan data lokal:

```powershell
docker compose down
```

Untuk menghapus seluruh data persisten dan memulai ulang dari keadaan kosong:

```powershell
docker compose down -v
```

Perintah terakhir menghapus data database, Kafka, Grafana, dan Airflow yang
disimpan di Docker volumes. File konfigurasi di repo tidak terhapus.
