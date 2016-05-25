create database if not exists xproto;
use xproto;
drop table if exists field_test;
create table field_test (
  my_int_u INT UNSIGNED NOT NULL
, my_int INT
, my_tinyint_u TINYINT UNSIGNED
, my_tinyint TINYINT
, my_smallint_u SMALLINT UNSIGNED
, my_smallint SMALLINT
, my_mediumint_u MEDIUMINT UNSIGNED
, my_mediumint MEDIUMINT
, my_bigint_u BIGINT UNSIGNED
, my_bigint BIGINT
, my_double DOUBLE
, my_float FLOAT
, my_decimal DECIMAL(10,2)
, my_varchar VARCHAR(123)
, my_char CHAR(5)
, my_varbinary VARBINARY(23)
, my_binary BINARY(3)
, my_blob BLOB(27)
, my_text TEXT(32)
, my_geometry GEOMETRY
, my_time TIME
, my_date DATE
, my_datetime DATETIME
, my_datetime_m DATETIME(3)
, my_year YEAR
, my_timestamp TIMESTAMP
, my_timestamp_m TIMESTAMP(6) NULL -- DEFAULT CURRENT_TIMESTAMP(6)
, my_set SET ('1', '2', '3')
, my_enum ENUM ('r', 'g', 'b')
, my_bit BIT(17)
, PRIMARY KEY (my_int_u)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into field_test VALUES (
  1234567890
, -987654321
, 43
, -123
, 23456
, -32198
, 8123456
, -7654321
, 92233720368547
, -92233720368547
, 123.45678910234
, -6543.394
, 10101.99
, 'abc 123 def 456'
, 'edcba'
, "\4\2\7\0\b"
, "\8\5\3"
, "\5\65\3\8\24\0\0\3"
, "hello, world!"
, ST_GeomFromText('POINT(1 1)')
, '-32:43:54'
, '2011-01-03'
, '2016-05-20 19:52:33'
, '2016-05-20 19:52:33'
, 1999
, '2016-05-20 19:53:43'
, '2016-05-20 19:53:43'
, '3,2'
, 'g'
, b'1010101010101'
),
(
  234567890
, -98765431
, 45
, -13
, 23756
, -32118
, 8123056
, -7650321
, 922331720368547
, -922333720368547
, 122.46789102341
, -43.391
, 12101.9
, 'un deux trois'
, 'zyxwv'
, "\42\24\74\0\b\n\n\n"
, "\2\1\4"
, "\5\65\4\8\24\1\0\3"
, "bye!"
, ST_GeomFromText('POINT(2 6)')
, '12:43:54'
, '2001-01-07'
, '2016-05-20 20:01:33.123'
, '2016-05-20 20:01:33.123'
, 1984
, '2016-05-20 20:02:43.000001'
, '2016-05-20 20:02:43.000001'
, '2,1'
, 'b'
, b'010101010101'
),
(
  4200000000
, -98765431
, 45
, -13
, 23756
, -32118
, 8123056
, -7650321
, 922331720368547
, -922333720368547
, 122.46789102341
, -43.391
, 12101.9
, 'un deux trois'
, 'zyxwv'
, "\42\24\74\0\b\n\n\n"
, "\2\1\4"
, "\5\65\4\8\24\1\0\3"
, "bye!"
, ST_GeomFromText('POINT(2 6)')
, '12:43:54'
, '2001-01-07'
, '2016-05-20 00:00:00'
, '2016-05-20 00:00:00'
, 1984
, '2016-05-20 00:00:00'
, '2016-05-20 00:00:00'
, '2,1'
, 'b'
, b'010101010101'
);
