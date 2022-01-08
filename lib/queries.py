## query must be dictionary !!

init_query = """drop table if exists accident, assaults, damageds,accident_type, accident_type_l, involved_types, casualty, involved, road_type, road_type_l, violations, weather, location;
"""

create_tables = {'accident': """CREATE TABLE accident
(
    accident_id SERIAL PRIMARY KEY,
    occr_date   TIMESTAMP,
    daynight    VARCHAR(50),
    wday        VARCHAR(50),
    death       INT,
    serious     INT,
    injured     INT,
    slight      INT,
    wound       INT,
    sido        VARCHAR(50),
    sgg         VARCHAR(50),
    acc_type_l  VARCHAR(50),
    acc_type    VARCHAR(50),
    violation   VARCHAR(50),
    road_form_l VARCHAR(50),
    road_form   VARCHAR(50),
    assault     VARCHAR(50),
    damaged     VARCHAR(50)
)""", 'weather': """CREATE TABLE weather
(
    weather_id         SERIAL PRIMARY KEY,
    area_id            varchar(50),
    area_nm            varchar(50),
    measure_dt         timestamp,
    temperature        decimal,
    rainfall           decimal,
    wind_speed         decimal,
    wind_direction     decimal,
    humidity           decimal,
    snow_drifts        decimal,
    ground_temperature decimal
)""", 'assualts': """CREATE TABLE assaults
(
    assault_id	SERIAL	 PRIMARY KEY ,
    assault		VARCHAR(20)
)""", 'damageds': """CREATE TABLE damageds
(
    damaged_id	SERIAL	 PRIMARY KEY ,
    damaged		VARCHAR(20)
)""", 'involved_types': """CREATE TABLE involved_types
(
    accident_id		INT	REFERENCES accident,
    assault_id		INT REFERENCES assaults,
    damaged_id		INT REFERENCES damageds,
	PRIMARY KEY 	(accident_id)
)""", 'location': """CREATE TABLE location
(
    location_id SERIAL PRIMARY KEY,
    sido        VARCHAR(50),
    sgg         VARCHAR(50)
)""", 'casualty': """CREATE TABLE casualty
(
    accident_id INTEGER REFERENCES accident,
    death       INT,
    serious     INT,
    injured     INT,
    slight      INT,
    wound       INT,
    PRIMARY KEY (accident_id)
)""", 'violations': """create table violations
(
    violation_id serial primary key,
    violation    varchar(50)
)""", 'road_type_l': """create table road_type_l
(
    road_type_l_id serial primary key,
    road_form_l    varchar(50) not null
)""", 'road_type': """create table road_type
(
    road_type_id   serial primary key,
    road_type_l_id integer,
    road_form      varchar(50) not null
)""", 'accident_type_l': """create table accident_type_l
(
    acc_type_l_id SERIAL PRIMARY KEY,
    acc_type_l    VARCHAR(50)
)""", 'accident_type': """create table accident_type
(
    acc_type_id   SERIAL PRIMARY KEY,
    acc_type_l_id integer,
    acc_type      VARCHAR(50)
)""", }

# accident 주/야 => 1/2
accident_daynight_update = {
    1: """UPDATE accident SET daynight = '1' 				 
WHERE daynight = '주간' OR daynight = '주';
""", 2: """UPDATE accident SET daynight = '2'
WHERE daynight = '야간' OR daynight = '야';
ALTER TABLE accident ALTER COLUMN daynight TYPE INTEGER USING daynight::integer;
"""
}

# involved_types, assaults, damageds 테이블 관련
involved_types_assaults_damageds_insert = {
    1: """INSERT INTO involved_types (accident_id)
	SELECT accident_id
	FROM accident;
""", 2: """INSERT INTO assaults (assault)
	SELECT DISTINCT assault
	FROM accident;
""", 3: """INSERT INTO damageds (damaged)
	SELECT DISTINCT damaged
	FROM accident;
""", 4: """ALTER TABLE accident ADD COLUMN assault_id INTEGER;
ALTER TABLE accident ADD COLUMN damaged_id INTEGER;
""", 5: """update accident as o
set assault_id = a.assault_id
from assaults as a
where a.assault = o.assault;

update accident as o
set damaged_id = a.damaged_id
from damageds as a
where a.damaged = o.damaged;

update involved_types as o
	set assault_id = a.assault_id
	from accident a
	where o.accident_id = a.accident_id;
	
update involved_types as o
	set damaged_id = a.damaged_id
	from accident a
	where o.accident_id = a.accident_id;"""
}

# location 관련
location = {
    1: """INSERT INTO location(sido, sgg)
SELECT DISTINCT a.sido, a.sgg
FROM accident a;

ALTER TABLE accident
    ADD COLUMN location_id INTEGER; 

UPDATE accident a
SET location_id = l.location_id
FROM location l
WHERE (l.sido = a.sido AND l.sgg = a.sgg);

ALTER TABLE accident
    DROP
        COLUMN sido,
    DROP
        COLUMN sgg;"""
}

# casualty 관련
casualty = {
    1: """
INSERT INTO casualty (accident_id)
SELECT accident_id
FROM accident;

UPDATE casualty c
SET death = a.death
FROM accident a
WHERE c.accident_id = a.accident_id;
UPDATE casualty c
SET serious = a.serious
FROM accident a
WHERE c.accident_id = a.accident_id;
UPDATE casualty c
SET injured = a.injured
FROM accident a
WHERE c.accident_id = a.accident_id;
UPDATE casualty c
SET slight = a.slight
FROM accident a
WHERE c.accident_id = a.accident_id;
UPDATE casualty c
SET wound = a.wound
FROM accident a
WHERE c.accident_id = a.accident_id;

ALTER TABLE accident
    DROP COLUMN death,
    DROP COLUMN serious,
    DROP COLUMN injured,
    DROP COLUMN slight,
    DROP COLUMN wound;"""
}

# violation 관련
violation = {1: """alter table accident
    add column violation_id integer;
insert into violations(violation)
select distinct violation
from accident;
update accident as o
set violation_id = v.violation_id
from violations as v
where o.violation = v.violation;
alter table accident
    drop column violation;
alter table accident
    add constraint constraint_accident_violations_fk foreign key (violation_id) references violations (violation_id);
"""}

# road_type_l, road_type 관련
road_type_and_l = {1: """alter table accident
    add column road_type_l_id integer;
alter table accident
    add column road_type_id integer;

insert into road_type_l(road_form_l)
select distinct a.road_form_l
from accident as a;
insert into road_type(road_form)
select distinct a.road_form
from accident as a;

update accident as o
set road_type_l_id = r.road_type_l_id
from road_type_l as r
where r.road_form_l = o.road_form_l;
update accident as o
set road_type_id = r.road_type_id
from road_type as r
where r.road_form = o.road_form;

update road_type as r
set road_type_l_id = a.road_type_l_id
from accident as a
where a.road_form = r.road_form
  and a.road_type_id = r.road_type_id;

alter table road_type
    alter column road_type_l_id set not null;

alter table accident
    drop column road_form_l;
alter table accident
    drop column road_form;

alter table accident
    add constraint constraint_accident_roadTypeL_fk foreign key (road_type_l_id) references road_type_l (road_type_l_id);
alter table accident
    add constraint constraint_accident_roadType_fk foreign key (road_type_id) references road_type (road_type_id);
alter table road_type
    add constraint constraint_roadType_roadTypeL_fk foreign key (road_type_l_id) references road_type_l (road_type_l_id);"""}

# acc_type 관련
acc_type = {1:"""insert into accident_type_l(acc_type_l)
select distinct acc_type_l
from accident;
insert into accident_type(acc_type)
select distinct acc_type
from accident;

alter table accident
    add column acc_type_l_id integer;
alter table accident
    add column acc_type_id integer;

update accident as o
set acc_type_l_id = a.acc_type_l_id
from accident_type_l as a
where a.acc_type_l = o.acc_type_l;
update accident as o
set acc_type_id = a.acc_type_id
from accident_type as a
where a.acc_type = o.acc_type;
update accident_type as o
set acc_type_l_id = a.acc_type_l_id
from accident as a
where a.acc_type_id = o.acc_type_id
  and a.acc_type = o.acc_type;

alter table accident
    drop column acc_type;
alter table accident
    drop column acc_type_l;

alter table accident
    alter column acc_type_id set not null;
alter table accident
    alter column acc_type_l_id set not null;
alter table accident_type
    alter column acc_type_l_id set not null;

alter table accident_type
    add constraint constraint_accidentType_accidentTypeL_fk foreign key (acc_type_l_id) references accident_type_l (acc_type_l_id);

alter table accident
    add constraint constraint_accident_accidentTypeL_fk foreign key (acc_type_l_id) references accident_type_l (acc_type_l_id);
alter table accident
    add constraint constraint_accident_accidentType_fk foreign key (acc_type_id) references accident_type (acc_type_id);"""}

extra_fk = {1:"""alter table accident
    add constraint assault_fk foreign key (assault_id) references assaults (assault_id);
alter table accident
    add constraint damaged_fk foreign key (damaged_id) references damageds (damaged_id);
alter table accident
    add constraint location_fk foreign key (location_id) references location (location_id);"""}

all_query = {
    1:"""select *
from accident as a
         join accident_type t on t.acc_type_id = a.acc_type_id
         join accident_type_l atl on atl.acc_type_l_id = a.acc_type_l_id
         join assaults a2 on a2.assault_id = a.assault_id
         join casualty c on a.accident_id = c.accident_id
         join damageds d on d.damaged_id = a.damaged_id
         join violations v on v.violation_id = a.violation_id
         join road_type rt on rt.road_type_id = a.road_type_id
         join road_type_l rtl on a.road_type_l_id = rtl.road_type_l_id
         join involved_types i on a.accident_id = i.accident_id
         join location l on l.location_id = a.location_id
         join weather w on (w.area_nm = l.sgg and w.measure_dt = a.occr_date)
"""
}