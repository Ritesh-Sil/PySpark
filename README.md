
# PySpark

A brief description of what this project does and who it's for


## Documentation : Common key points and issues.

Key Point : PySpark main driver program

```
spark = SparkSession.builder.master('local[1]/yarn').appName('<App name>').getOrCreate()
```
- For the master `local` is used for the standalone mode.Number inside the `[x]` means the number of partitions to be created.
- To run the PySpark application in a hadoop cluster the master will be `yarn`, if we choose yarn as a resource manager.
- To run the PySpark application to read the tables from hive, we can use `enableHiveSupport()` option in the driver program.

Key Point : Using Spark sql

```
df = spark.sql("<Mention the SQL query here>")
df.show()
```

Key Point : Proper connection setup from pyspark to redshift database using jdbc
```
df = spark.read.format('jdbc')\
	.option('url','jdbc:postgresql://<the amazon database instance>.redshift.amazonaws.com:5439/<database>?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory')\
	.option('dbtable','<db_schema>.<table>')\
	.option('user','<user_name>')\
	.option('password','<password>')\
	.option('driver','com.amazon.redshift.jdbc41.Driver')\
	.load()
```

Key Point : Explicitly Mentioning the .jar file in spark-shell (for redshift)
```
pyspark --jars RedshiftJDBC41-1.1.10.1010.jar 
```

Issue :
```
py4j.protocol.Py4JJavaError: An error occurred while calling o321.load.
: java.lang.ClassNotFoundException: org.postgresql.Driver
```

Solution: The reason for this issue is, the JVM is unable to locate the class ```org.postgresql.Driver```, which means 
it is simply unable to locate the `.jar` file where the class is mentioned.


Issue : 
```
py4j.protocol.Py4JJavaError: An error occurred while calling o389.load.
: java.sql.SQLException: [Amazon](500150) Error setting/closing connection: no pg_hba.conf entry for host "::ffff:<ip address of the arn>", user "<user_name>", database "<database>", SSL off.
        at com.amazon.redshift.client.PGClient.<init>(Unknown Source)
```
Solution : 
[Stackoverflow reference](https://stackoverflow.com/questions/36617560/amazon-500150-error-setting-closing-connection-general-sslengine)

