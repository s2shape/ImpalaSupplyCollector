drop table if exists test_data_types;
create table test_data_types (
   id int,
   tinyint_field tinyint,
   int_field int,
   bigint_field bigint,
   bool_field boolean,
   float_field float,
   double_field double,
   decimal_field decimal(3,2),
   string_field string,
   char_field char(40),
   timestamp_field timestamp
);

drop table if exists test_field_names;
create table test_field_names (
   id int,
   low_case int,
   UPCASE int,
   CamelCase int,
   `Table` int,
   `array` int,
   `SELECT` int
);

insert into test_field_names(id, low_case, upcase, camelcase, `table`, `array`, `select`)
values(1,0,0,0,0,0,0);

DROP TABLE IF EXISTS test_index;
CREATE TABLE IF NOT EXISTS test_index(id int, name string);
INSERT INTO test_index(id, name) VALUES(1, 'Sunday');
INSERT INTO test_index(id, name) VALUES(2, 'Monday');
INSERT INTO test_index(id, name) VALUES(3, 'Tuesday');
INSERT INTO test_index(id, name) VALUES(4, 'Wednesday');
INSERT INTO test_index(id, name) VALUES(5, 'Thursday');
INSERT INTO test_index(id, name) VALUES(6, 'Friday');
INSERT INTO test_index(id, name) VALUES(7, 'Saturday');

