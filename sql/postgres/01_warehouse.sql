CREATE TABLE IF NOT EXISTS dim_building (
  building_key SERIAL PRIMARY KEY,
  building_id VARCHAR(20) UNIQUE NOT NULL,
  building_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_device (
  device_key SERIAL PRIMARY KEY,
  device_id VARCHAR(30) UNIQUE NOT NULL,
  device_type VARCHAR(30) NOT NULL,
  building_id VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
  time_key SERIAL PRIMARY KEY,
  full_timestamp TIMESTAMP UNIQUE NOT NULL,
  date_value DATE NOT NULL,
  hour_value SMALLINT NOT NULL,
  day_name VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_alert_type (
  alert_type_key SERIAL PRIMARY KEY,
  alert_code VARCHAR(50) UNIQUE NOT NULL,
  alert_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_device_metric_hourly (
  metric_id BIGSERIAL PRIMARY KEY,
  time_key INT REFERENCES dim_time(time_key),
  building_key INT REFERENCES dim_building(building_key),
  device_key INT REFERENCES dim_device(device_key),
  avg_traffic_mbps NUMERIC(12,3),
  max_traffic_mbps NUMERIC(12,3),
  avg_clients NUMERIC(12,3),
  avg_latency_ms NUMERIC(12,3),
  avg_packet_loss_pct NUMERIC(8,3),
  quality_rejected_count INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_sla_window (
  sla_id BIGSERIAL PRIMARY KEY,
  time_key INT REFERENCES dim_time(time_key),
  building_key INT REFERENCES dim_building(building_key),
  latency_p95_ms NUMERIC(12,3),
  packet_loss_pct NUMERIC(8,3),
  sla_compliance_pct NUMERIC(8,3),
  compliant BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_alert (
  alert_id BIGSERIAL PRIMARY KEY,
  time_key INT REFERENCES dim_time(time_key),
  building_key INT REFERENCES dim_building(building_key),
  device_key INT REFERENCES dim_device(device_key),
  alert_type_key INT REFERENCES dim_alert_type(alert_type_key),
  severity VARCHAR(20) NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_data_quality (
  quality_id BIGSERIAL PRIMARY KEY,
  time_key INT REFERENCES dim_time(time_key),
  issue_type VARCHAR(50) NOT NULL,
  inserted_count INT NOT NULL,
  caught_count INT NOT NULL
);
