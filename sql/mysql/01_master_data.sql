CREATE DATABASE IF NOT EXISTS network_master;
USE network_master;

CREATE TABLE dim_building (
  building_id VARCHAR(20) PRIMARY KEY,
  building_name VARCHAR(100) NOT NULL,
  sla_latency_ms INT NOT NULL DEFAULT 100,
  sla_loss_pct DECIMAL(5,2) NOT NULL DEFAULT 2.00
);

CREATE TABLE dim_device (
  device_id VARCHAR(30) PRIMARY KEY,
  device_type VARCHAR(30) NOT NULL,
  building_id VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  FOREIGN KEY (building_id) REFERENCES dim_building(building_id)
);

CREATE TABLE dim_threshold (
  threshold_name VARCHAR(50) PRIMARY KEY,
  threshold_value DECIMAL(10,3) NOT NULL,
  unit VARCHAR(20) NOT NULL
);

INSERT INTO dim_building VALUES
('GEDUNG-A','Gedung A',100,2.00),
('GEDUNG-B','Gedung B',100,2.00);

INSERT INTO dim_threshold VALUES
('latency_p95_ms',100.000,'ms'),
('packet_loss_pct',2.000,'pct'),
('heartbeat_seconds',120.000,'seconds'),
('moving_average_sigma',3.000,'sigma');

DELIMITER //
CREATE PROCEDURE seed_network_devices()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 8 DO
    INSERT INTO dim_device VALUES
      (CONCAT('SW-', LPAD(i,2,'0')), 'switch',
       IF(i <= 4,'GEDUNG-A','GEDUNG-B'), 'active');
    SET i = i + 1;
  END WHILE;
  SET i = 1;
  WHILE i <= 12 DO
    INSERT INTO dim_device VALUES
      (CONCAT('AP-', LPAD(i,2,'0')), 'access_point',
       IF(i <= 6,'GEDUNG-A','GEDUNG-B'), 'active');
    SET i = i + 1;
  END WHILE;
  SET i = 1;
  WHILE i <= 4 DO
    INSERT INTO dim_device VALUES
      (CONCAT('PROBE-', LPAD(i,2,'0')), 'latency_probe',
       IF(i <= 2,'GEDUNG-A','GEDUNG-B'), 'active');
    SET i = i + 1;
  END WHILE;
END //
DELIMITER ;

CALL seed_network_devices();
