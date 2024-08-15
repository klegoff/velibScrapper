CREATE TABLE IF NOT EXISTS historic (
  record_id VARCHAR NOT NULL,
  stationcode VARCHAR,
  ebike SMALLINT,
  mechanical SMALLINT,
  numbikesavailable SMALLINT,
  numdocksavailable SMALLINT,
  is_renting BOOL,
  is_installed BOOL,
  is_returning BOOL,
  duedate timestamp,
  PRIMARY KEY (record_id)
  );

CREATE TABLE IF NOT EXISTS station (
  stationcode VARCHAR,
  name VARCHAR,
  nom_arrondissement_communes VARCHAR,
  capacity SMALLINT,
  coordonnee_x float,
  coordonnee_y float,
  PRIMARY KEY (stationcode)
  );