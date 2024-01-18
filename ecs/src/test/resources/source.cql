CREATE TABLE test_table(
    col1 varchar,
    col2 int,
    col3 text,
    col4 date,
    col5 timestamp,
    col6 float,
    col7 decimal,
    col8 bigint,
    col9 set<text>,
    col10 set<int>,
    col11 list<int>,
    col12 list<text>,
    col13 uuid,
    col14 timeuuid,
    col15 blob,
    col16 set<blob>,
    col17 list<blob>,
    PRIMARY KEY(col1,col2));

INSERT INTO test_table(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17) values('1234',20220701,'Eugen','2022-01-01','2017-04-01T11:21:59.001+0000',0.9,0.11112,99991111,{'ABC','CDE'},{1,2,3},[1,2,3],['ABC','CDE'], 9e7790b0-5938-11ed-bd0d-6d2c86545d91,b805c560-5938-11ed-bd0d-6d2c86545d91,0x54455854,{0x54455854,0x5445585431},[0x54455854,0x5445585431]);

CREATE TABLE test_counter(
    col1 varchar,
    col2 int,
    col3 counter,
    PRIMARY KEY(col1,col2));

UPDATE test_counter SET col3 = col3 + 1 WHERE col1='TEST1' and col2=1;