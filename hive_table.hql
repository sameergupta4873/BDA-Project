CREATE EXTERNAL TABLE IF NOT EXISTS hive_sensordata (
  timestamp STRING,
  device_id INT,
  temperature FLOAT,
  humidity FLOAT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = ":key,metrics:device_id,metrics:temperature,metrics:humidity"
)
TBLPROPERTIES ("hbase.table.name" = "sensordata");
