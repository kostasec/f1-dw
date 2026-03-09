Table dim.driver {
  driver_key int [primary key]
  driver_ref varchar 
  driver_number int
  driver_code varchar
  driver_forename varchar
  driver_surname varchar
  driver_dob date
  driver_nationality varchar
  driver_url nvarchar
  created_at date
  source varchar
}


Table dim.constructor {
  constructor_key integer [primary key]
  constructor_ref varchar
  constructor_name varchar
  constructor_nationality varchar
  constructor_url nvarchar
  created_at date
  source varchar
}


Table dim.circuit {
  circuit_key int [primary key]
  circuit_references varchar
  circuit_name varchar
  circuit_city varchar
  circuit_country varchar
  circuit_latitude decimal
  circuit_longitude decimal
  circuit_altitude decimal
  circuit_url nvarchar
  created_at date
  source varchar
}


Table dim.race {
  race_key int [primary key]
  circuit_key int
  race_name varchar
  race_year int
  race_round int
  race_date date
  race_time datetime
  fp1_date date
  fp1_time datetime
  fp2_date  date
  fp2_time datetime
  fp3_date date
  fp3_time datetime
  qualification_date date
  qualification_time datetime
  sprint_date date
  sprint_time datetime
  race_url nvarchar
  created_at datetime
  source varchar
}


Table dim.time {
  time_key int [primary key]
  full_date date
  year int
  month int
  day_of_month int
  day_of_week int
  day_name varchar
  day_name_short varchar
  month_name varchar
  quarter varchar
  week_of_month int
  is_weekend boolean
  is_weekday boolean
}

Table fact.race_results {
  result_key int [primary key]
  race_key int 
  driver_key int
  constructor_key int 
  time_key int 
  number int
  grid int
  position int
  position_text varchar
  points int
  number_of_laps int
  rank int
  race_duration_hours varchar
  race_duration_miliseconds double
  fastest_lap int
  fastest_lap_minutes double
  fastest_lap_speed double
  status varchar
  year int
  created_at datetime
  source varchar
}

Table fact.laps {
  lap_key int [primary key]
  race_key int
  lap int 
  driver_key int
  constructor_key int
  time_key id
  grid int
  lap_position int
  lap_minutes varchar
  lap_miliseconds bignint
  created_at datetime
  source varchar
}

Table fact.pitstops {
  pitstop_key int [primary key]
  race_key int
  driver_key int
  stop int
  constructor_key int
  time_key id
  lap int
  pitlane_duration_seconds int
  pitlane_duration_milliseconds varchar
  created_at datetime
  source varchar
}

Table fact.driver_standings {
  ds_key int [primary key]
  race_key int
  driver_key int
  constructor_key int
  time_key int
  ds_points decimal 
  ds_position int
  ds_wins int
  created_at datetime
  source varchar 
}

Table fact.constructor_standings {
  cs_key int [primary key]
  race_key int
  constructor_key int
  time_key int
  cs_points decimal 
  cs_position int
  cs_wins int
  created_at datetime
  source varchar 
}

Ref race_circuit                     : dim.circuit.circuit_key > dim.race.circuit_key

//FACT_RACE_RESULTS
Ref race_fact_race_results           : dim.race.race_key > fact.race_results.race_key
Ref driver_fact_race_results         : dim.driver.driver_key > fact.race_results.driver_key
Ref constructor_fact_race_results    : dim.constructor.constructor_key > fact.race_results.constructor_key
Ref time_fact_race_results           : dim.time.time_key > fact.race_results.time_key

//FACT_LAPS
Ref race_fact_laps                   : dim.race.race_key > fact.laps.race_key
Ref driver_fact_laps                 : dim.driver.driver_key > fact.laps.driver_key
Ref constructor_fact_laps            : dim.constructor.constructor_key > fact.laps.constructor_key
Ref time_fact_laps                   : dim.time.time_key > fact.laps.time_key


//FACT_PITSTOP
Ref race_fact_pitstops              : dim.race.race_key > fact.pitstops.race_key
Ref driver_fact_pitstops            : dim.driver.driver_key > fact.pitstops.driver_key
Ref constructor_fact_pitstops       : dim.constructor.constructor_key > fact.pitstops.constructor_key
Ref time_fact_pitstops              : dim.time.time_key > fact.pitstops.time_key

//FACT_DRIVER_STANDINGS
Ref race_fact_ds                    : dim.race.race_key > fact.driver_standings.race_key
Ref driver_fact_ds                  : dim.driver.driver_key > fact.driver_standings.driver_key
Ref constructor_fact_ds             : dim.constructor.constructor_key > fact.driver_standings.constructor_key

//FACT_CONSTRUCTOR_STANDINGS
Ref race_fact_cs                    : dim.race.race_key > fact.constructor_standings.race_key
Ref driver_fact_cs                  : dim.constructor.constructor_key > fact.constructor_standings.constructor_key


Ref: "dim"."constructor"."constructor_key" < "dim"."constructor"."constructor_nationality"