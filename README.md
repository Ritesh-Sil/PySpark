
## Documentation : Common key points and issues.


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

Solution: The reason for this issue is, the JVM is unable to locate the class ```orr.postgresql.Driver```, which means 
it is simply unable to locate the `.jar` file where the class is mentioned.


Issue : 
```
py4j.protocol.Py4JJavaError: An error occurred while calling o389.load.
: java.sql.SQLException: [Amazon](500150) Error setting/closing connection: no pg_hba.conf entry for host "::ffff:<ip address of the arn>", user "<user_name>", database "<database>", SSL off.
        at com.amazon.redshift.client.PGClient.<init>(Unknown Source)
```
Solution : 
[Stackoverflow reference](https://stackoverflow.com/questions/36617560/amazon-500150-error-setting-closing-connection-general-sslengine)

