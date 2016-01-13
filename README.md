# JsonSerDe
Json SerDe in Hive

hive> list jars;
/home/ubuntu/hiveJars/JsonSerDe-1.0.jar

/home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar

hive> DELETE JAR /home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar;

Deleted [/home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar] from class path

hive> DELETE JAR /home/ubuntu/hiveJars/JsonSerDe-1.0.jar;

Deleted [/home/ubuntu/hiveJars/JsonSerDe-1.0.jar] from class path

hive> ADD JAR /home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar;

Added [/home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar] to class path

Added resources: [/home/ubuntu/hive-1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar]

hive> ADD JAR /home/ubuntu/hiveJars/JsonSerDe-1.0.jar;

Added [/home/ubuntu/hiveJars/JsonSerDe-1.0.jar] to class path

Added resources: [/home/ubuntu/hiveJars/JsonSerDe-1.0.jar]

hive> drop table thanooj.empjson;
OK
Time taken: 0.154 seconds

hive> CREATE TABLE thanooj.EmpJson ( name STRING, salary FLOAT, mobile BIGINT, location STRING) ROW FORMAT SERDE 'com.hive.serde.json.MyJsonSerDe';

OK

Time taken: 0.094 seconds

hive> LOAD DATA LOCAL INPATH '/home/ubuntu/input/EmpJson.txt' OVERWRITE INTO TABLE THANOOJ.EmpJson;

Loading data to table thanooj.empjson

Table thanooj.empjson stats: [numFiles=1, numRows=0, totalSize=233, rawDataSize=0]

OK

Time taken: 0.261 seconds

hive> select * from thanooj.empjson;

OK

sriram	5000.0	1234567890	ayodhya

seetha	4500.0	9876543210	midhila

lakshman	2000.0	5432167890	ayodhya

Time taken: 0.08 seconds, Fetched: 3 row(s)

hive> insert overwrite local directory '/home/ubuntu/jsonInput/' ROW FORMAT SERDE 'com.hive.serde.json.MyJsonSerDe' select * from thanooj.EmpJson;

Query ID = ubuntu_20160113190947_ae2318c2-8a79-47a9-a904-99da3e344eb9

Total jobs = 1

Launching Job 1 out of 1

Number of reduce tasks is set to 0 since there's no reduce operator

Starting Job = job_1452676975570_0014, Tracking URL = http://ubuntu:8088/proxy/application_1452676975570_0014/

Kill Command = /usr/local/hadoop2/bin/hadoop job  -kill job_1452676975570_0014

Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0

2016-01-13 19:09:53,171 Stage-1 map = 0%,  reduce = 0%

2016-01-13 19:09:59,609 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 0.71 sec

MapReduce Total cumulative CPU time: 710 msec

Ended Job = job_1452676975570_0014

Copying data to local directory /home/ubuntu/jsonInput

Copying data to local directory /home/ubuntu/jsonInput

MapReduce Jobs Launched: 

Stage-Stage-1: Map: 1   Cumulative CPU: 0.71 sec   HDFS Read: 3504 HDFS Write: 215 SUCCESS

Total MapReduce CPU Time Spent: 710 msec

OK

Time taken: 13.528 seconds

hive>
