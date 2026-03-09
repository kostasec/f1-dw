/*VIEW RACES_CIRCUITS*/
CREATE VIEW RACES_CIRCUITS AS(
SELECT 
    race_key,
    race_id,
    race_name,
    r.circuit_key,
    circuit_references,
    race_year,
    race_round,
    CAST(race_date AS DATE),
    race_time,
    COALESCE(CAST(CAST(fp1_date AS DATE) AS VARCHAR),        		'not held') AS fp1_date,
    COALESCE(CAST(fp1_time AS VARCHAR),        						'not held') AS fp1_time,
    COALESCE(CAST(CAST(fp2_date AS DATE) AS VARCHAR),        		'not held') AS fp2_date,
    COALESCE(CAST(fp2_time AS VARCHAR),        						'not held') AS fp2_time,
    COALESCE(CAST(CAST(fp3_date AS DATE) AS VARCHAR),        		'not held') AS fp3_date,
    COALESCE(CAST(fp3_time AS VARCHAR),        						'not held') AS fp3_time,
    COALESCE(CAST(CAST(qualification_date AS DATE) AS VARCHAR), 	'not held') AS qualification_date,
    COALESCE(CAST(qualification_time AS VARCHAR), 					'not held') AS qualification_time,
    COALESCE(CAST(CAST(sprint_date AS DATE) AS VARCHAR),     		'not held') AS sprint_date,
    COALESCE(CAST(sprint_time AS VARCHAR),     						'not held') AS sprint_time,
    race_url
FROM dimensions.race r 
JOIN dimensions.circuit c ON r.circuit_key = c.circuit_key
)
/*there's no dramatic benefits of putting join indexes here as circuit_key is already a primary key therefore it already has an index on itself*/

/*VIEW RESULTS WITH DIMENSIONS*/
CREATE VIEW RESULTS AS(
SELECT result_key, result_id, f.race_key, r.race_name, c.circuit_key, c.circuit_references, f.driver_key,f.driver_number, d.driver_forename||' '||d.driver_surname AS driver_name, f.constructor_key,cc.constructor_ref, f.year, CAST(t.full_date AS DATE), grid, position, points, rank, number_of_laps,
race_duration_hours, fastest_lap, fastest_lap_minutes, fastest_lap_speed, status
FROM facts.race_results f
JOIN dimensions.race r
ON r.race_key = f.race_key
JOIN dimensions.circuit c
ON c.circuit_key = r.circuit_key
JOIN dimensions.driver d
ON d.driver_key = f.driver_key
JOIN dimensions.constructor cc
ON cc.constructor_key = f.constructor_key
JOIN dimensions.time t
ON t.time_key = f.time_key
WHERE d.driver_forename='Lewis'
)
/*VIEW LAPS WITH DIMENSIONS*/
CREATE VIEW LAPS AS(
SELECT lap_key, f.race_key, r.race_name, c.circuit_key, c.circuit_references, lap, f.driver_key,d.driver_forename||' '||d.driver_surname AS driver_name, f.constructor_key, cc.constructor_ref, CAST(t.full_date AS DATE), lap_position, lap_minutes, lap_milliseconds
FROM facts.laps f
JOIN dimensions.race r
ON r.race_key = f.race_key
JOIN dimensions.circuit c
ON c.circuit_key = r.circuit_key
JOIN dimensions.driver d
ON d.driver_key = f.driver_key
JOIN dimensions.constructor cc
ON cc.constructor_key = f.constructor_key
JOIN dimensions.time t
ON t.time_key = f.time_key
)
/*VIEW PITSTOPS WITH DIMENSIONS*/
CREATE VIEW PITSTOPS AS (
SELECT pitstop_key, f.race_key, r.race_name, f.driver_key, d.driver_forename||' '||d.driver_surname AS driver_name, stop, f.constructor_key, cc.constructor_ref, CAST(t.full_date AS DATE), local_time, pitlane_duration_seconds 
FROM facts.pitstops f
JOIN dimensions.race r
ON r.race_key = f.race_key
JOIN dimensions.driver d
ON d.driver_key = f.driver_key
JOIN dimensions.constructor cc
ON cc.constructor_key = f.constructor_key
JOIN dimensions.time t
ON t.time_key = f.time_key
)
/*VIEW DRIVER_STANDINGS WITH DIMENSIONS*/
CREATE VIEW DRIVER_STANDINGS AS(
SELECT ds_key, ds_id, f.race_key, r.race_name, f.driver_key, d.driver_forename||' '||d.driver_surname AS driver_name, f.constructor_key, cc.constructor_ref,  CAST(t.full_date AS DATE),  ds_position, ds_wins 
FROM facts.driver_standings f
JOIN dimensions.race r
ON r.race_key = f.race_key
JOIN dimensions.driver d
ON d.driver_key = f.driver_key
JOIN dimensions.constructor cc
ON cc.constructor_key = f.constructor_key
JOIN dimensions.time t
ON t.time_key = f.time_key
)
/*VIEW CONSTRUCTOR_STANDINGS WITH DIMENSIONS*/
CREATE VIEW CONSTRUCTOR_STANDINGS AS(
SELECT cs_key, cs_id, f.race_key, r.race_name, f.constructor_key, cc.constructor_name, CAST(t.full_date AS DATE), cs_position, cs_wins
FROM facts.constructor_standings f
JOIN dimensions.race r
ON r.race_key = f.race_key
JOIN dimensions.constructor cc
ON cc.constructor_key = f.constructor_key
JOIN dimensions.time t
ON t.time_key = f.time_key
)