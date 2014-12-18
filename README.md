This demo shows how you might sync data between 2 sources where data may be changing. 

The SparkSync job is responsible for validating some data in HDFS and updating the corresponding rows in DSE/Cassandra.

You will need to download the users.csv and put it in the src/main/resources folder NOTE this is 188MB - https://www.dropbox.com/s/6up1q4k2i2zgkym/users.csv?dl=0

This demo assumes you have dse4.6 installed with and access to hdfs system, I have been using the hortonworks sandbox of HDP 1.3 which can be downloaded from http://hortonworks.com/products/hortonworks-sandbox/ 

The idea behind the demo is assuming that we want to sync 2 data sources while one of them (Cassandra) is being constantly being updated. To ensure that new changes are not overwritten, the programs makes use of lightweight transactions (LWT). This allows us to check that the data is what we expect it to before we change it. If the value is not what we expected, we can decide to do something different with it. 

From CQLSH - we can load all the users into DSE/Cassandra

```cql
copy test.users from 'INSTALL_LOCATION/src/main/resources/users.csv';
```

update the same file, 'users.csv' to your hdfs. 

To mimic some changes that have happened on the front end, we will change some of the users phone numbers.

```cql
update test.users set phone ='99999999' where id = d3ebeb17-4f71-4496-a6d7-50e5c633c9ff;
update test.users set phone ='99999999' where id = 6e9e50fd-00dd-4e6a-a677-46c513948dd8;
```

To build the project run
```
sbt/sbt assembly
```
This produces a jar file that can be passed to the workers. 

To submit the job to the DSE spark master, notice I pass in the hdfs file location at the end
```
~/dse4.6/bin/dse spark-submit --class SparkSync --master=spark://127.0.0.1 ./target/scala-2.10/sparksync-assembly-0.1.jar hdfs://hortonworks.sandbox:8020/user/users.csv
```


