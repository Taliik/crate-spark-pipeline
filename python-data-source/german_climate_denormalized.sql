CREATE TABLE interns.raw_weather (
  date timestamp,
  station_id string,
  station_name string,
  position geo_point, -- position of the weather station
  station_height int, -- height of the weather station
  temp float, -- temperature in °C
  humility double, -- relative humility in percent
  cloudiness int,  -- 0 (cloudless)
                   -- 1 or less (nearly cloudless)
                   -- 2 (less cloudy)
                   -- 3
                   -- 4 (cloudy)
                   -- 5
                   -- 6 (more cloudy)
                   -- 7 or more (nearly overcast)
                   -- 8 (overcast)
                   -- -1 not available
  rainfall_fallen boolean, -- if some precipitation happened this hour
  rainfall_height double,  -- precipitation height in mm
  rainfall_form int, -- 0 - no precipitation
                     -- 1 - only "distinct" (german: "abgesetzte") precipitation
                     -- 2 - only liquid "distinct" precipitation (e.g. dew)
                     -- 3 - only solid "distinct" precipitation (e.g. frost)
                     -- 6 - liquid
                     -- 7 - solid
                     -- 8 - solid and liquid
                     -- 9 - no measurement
  air_pressure double,  -- air pressure (Pa)
  air_pressure_station_height double, -- air pressure at station height (Pa)
  ground_temp_2cm float, -- soil temperature in °C at 2cm depth
  ground_temp_5cm float, -- soil temperature in °C at 5cm depth
  ground_temp_10cm float, -- soil temperature in °C at 10cm depth
  ground_temp_20cm float, -- soil temperature in °C at 20cm depth
  ground_temp_50cm float, -- soil temperature in °C at 50cm depth
  ground_temp_100cm float, -- soil temperature in °C at 100cm depth
  sunshine_duration double, -- sum of sunshine duration in that hour in minutes
  diffuse_sky_radiation double, -- sum of diffuse short-wave sky-radiation in J/cm² for that hour
  global_radiation double, -- sum of global short-wave radiation in J/cm² for that hour
  sun_zenith float, -- solar zenith angle (https://en.wikipedia.org/wiki/Solar_zenith_angle) in degree
  wind_speed double, -- wind speed in m/sec
  wind_direction int -- wind direction given in 36-part land-spout
) clustered by (station_id) into 16 shards with (number_of_replicas=1, refresh_interval=0);
