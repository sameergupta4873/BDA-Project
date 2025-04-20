REGISTER '/usr/lib/pig/piggybank.jar';

data = LOAD '/user/hadoop/sensordata/' USING PigStorage(',') 
    AS (timestamp:chararray, device_id:int, temperature:float, humidity:float);

filtered = FILTER data BY temperature > 25;

grouped = GROUP filtered BY device_id;

avg_temp = FOREACH grouped GENERATE group AS device_id, AVG(filtered.temperature) AS avg_temperature;

STORE avg_temp INTO '/user/hadoop/output/avg_temperature' USING PigStorage(',');
