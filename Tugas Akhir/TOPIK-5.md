# Topik 5 — Data Center / Network Telemetry

Tim operasional TI ingin memantau kesehatan infrastruktur secara real-time, mendeteksi masalah sebelum berdampak ke pengguna, dan menganalisis kapasitas serta efisiensi dari data historis.

## Varian A — Rak Server

Ruang data center dengan beberapa baris rak server, CRAC (pendingin), dan PDU.

| Perangkat | Jumlah min. | Field | Rentang normal |
|---|---|---|---|
| Server | 16 | `cpu_pct`, `mem_pct`, `inlet_temp_c`, `outlet_temp_c`, `fan_rpm` | inlet 18–27 °C (ASHRAE) |
| PDU per rak | 4 | `power_kw`, `current_a` | — |
| CRAC / pendingin | 2 | `supply_temp_c`, `power_kw` | — |

**Tantangan signature — Deteksi *hotspot* dan perhitungan PUE**
- *Hotspot*: rak dengan rata-rata `inlet_temp_c` window 5 menit > 27 °C, atau selisih *outlet − inlet* jauh di atas rata-rata rak lain.
- **PUE** = total daya fasilitas (IT + pendingin) / daya IT, dihitung per jam dan per hari.
- Korelasi beban CPU dengan suhu dan daya ditampilkan di dashboard historis.

## Varian B — Jaringan Kampus

Jaringan kampus dengan switch per gedung dan access point WiFi.

| Perangkat | Jumlah min. | Field | Rentang normal |
|---|---|---|---|
| Switch | 8 | `port_in_mbps`, `port_out_mbps`, `errors`, `uptime_s` | utilisasi < 70 % |
| Access point | 12 | `clients`, `rssi_avg_dbm`, `channel_util_pct` | — |
| Probe latensi | 4 | `latency_ms`, `packet_loss_pct`, `target` | latensi < 50 ms, loss < 1 % |

Pola: trafik tinggi pada jam kuliah, rendah malam; sesekali ada *flash crowd* (mis. pendaftaran KRS).

**Tantangan signature — Pelanggaran SLA dan anomali trafik**
- SLA: alert bila latensi p95 window 5 menit > 100 ms atau packet loss > 2 %.
- Anomali trafik dengan *moving-average band*: rata-rata bergerak 1 jam ± 3 standar deviasi; trafik di luar band ditandai.
- Laporan ketersediaan SLA harian per gedung (persentase waktu patuh).

## Contoh Skema Pesan

```json
{
  "device_id": "SRV-R2-07",
  "device_type": "server",
  "rack_id": "RACK-02",
  "timestamp": "2026-11-20T09:15:02Z",
  "data": { "cpu_pct": 73.5, "mem_pct": 61.2, "inlet_temp_c": 24.8, "outlet_temp_c": 36.1, "fan_rpm": 8200 }
}
```

Field `device_id`, `device_type`, `timestamp`, dan `data` wajib ada, ditambah `rack_id` (Varian A) atau `building_id` (Varian B).

## Data Kotor yang Disarankan

- *Counter reset*: nilai kumulatif (`errors`, `uptime_s`) tiba-tiba kembali ke 0 setelah reboot.
- Persentase mustahil (`cpu_pct` = 140) atau suhu negatif.
- Satuan tidak konsisten (bps vs Mbps, °F vs °C).
- Perangkat berhenti mengirim (*missing heartbeat*) selama beberapa menit.

## KPI Streaming dan Alert

- Window 1 dan 5 menit: rata-rata/maksimum metrik per rak atau gedung.
- Alert real-time: kondisi signature dan perangkat tanpa heartbeat > 2 menit.
- Varian A: PUE berjalan. Varian B: jumlah klien WiFi per gedung.

## Saran Star Schema

- **Fakta**: `fact_device_metric_hourly`, `fact_sla_window` atau `fact_power_hourly`, `fact_alert`.
- **Dimensi**: `dim_device`, `dim_rack`/`dim_building`, `dim_time`, `dim_alert_type`.

## Kriteria Penerimaan

1. Minimal 20 perangkat dengan pola beban realistis dan minimal satu kejadian hotspot/flash crowd yang disimulasikan.
2. Tantangan signature (hotspot + PUE atau SLA + anomali) berjalan secara streaming dan tampil di dashboard.
3. Warehouse dapat menjawab: rak/gedung paling bermasalah selama 7 hari dan tren PUE atau ketersediaan SLA per hari.
4. Counter reset, satuan tidak konsisten, dan heartbeat hilang ditangani dan jumlahnya dilaporkan.
