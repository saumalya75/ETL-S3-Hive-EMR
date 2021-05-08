DROP TABLE rep_data;
create external table rep_data(
  rep_id Int,
  rep_name String,
  rep_rating double,
  rep_country String
)
row format delimited fields terminated by '|'
location 's3://generated-data-admin-ssarkar/sales-rep-data/RepData/'
tblproperties("skip.header.line.count"="1");

DROP TABLE call_data;
create external table call_data(
  call_value double,
  month_id int,
  rep_id int,
  call_id int
)
row format delimited fields terminated by '|'
location 's3://generated-data-admin-ssarkar/sales-rep-data/CallData/'
tblproperties("skip.header.line.count"="1");


DROP TABLE con_call_rep_data;
create external table con_call_rep_data(
  rep_id int,
  rep_name string,
  month_id int,
  total_call_value double
)
row format delimited fields terminated by '|'
location 's3://generated-data-admin-ssarkar/sales-rep-data/ConCallRepData/';
