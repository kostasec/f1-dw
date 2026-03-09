SELECT constructor_key, COUNT (*)
FROM dimensions.constructor
GROUP BY constructor_key
HAVING COUNT (*) > 1

SELECT driver_key, COUNT(*) 
FROM dimensions.driver
GROUP BY driver_key
HAVING COUNT(*) > 1

SELECT circuit_key, COUNT(*)
FROM dimensions.circuit
GROUP BY circuit_key
HAVING COUNT(*) > 1

SELECT race_key, COUNT (*)
FROM dimensions.race
GROUP BY race_key
HAVING COUNT(*) > 1



SELECT result_key, COUNT (*)
FROM facts.race_results
GROUP BY result_key
HAVING COUNT (*) > 1

SELECT pitstop_key, COUNT (*)
FROM facts.pitstops
GROUP BY pitstop_key
HAVING COUNT (*) > 1

SELECT lap_key, COUNT (*)
FROM facts.laps
GROUP BY lap_key
HAVING COUNT (*) > 1

SELECT ds_key, COUNT (*)
FROM facts.driver_standings
GROUP BY ds_key
HAVING COUNT (*) > 1

SELECT cs_key, COUNT (*)
FROM facts.constructor_standings
GROUP BY cs_key
HAVING COUNT (*) > 1