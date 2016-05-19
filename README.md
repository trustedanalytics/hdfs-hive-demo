# hdfs-hive-demo
Example of application using HDFS and HIVE with TAP security model.

The application provides simple API for accessing HDFS and HIVE.
Usage of the API is demonstrated by interactive scripts.

0. Prerequisites
  - Installed jq tool (sudo apt-get install jq)
  - Access to TAP instance with Kerberos 

We have created a simple scenario consisting of few steps.

1. Login into the TAP. 
```bash
cf login -a <endpoint> -u <user> -o <org> -s <space>
```
Make sure you have a space developer role.
```bash
cf space-users <org> <space>
```
2. Run interactive script ./setup.sh 
The script will create instances of 4 services: 
  - hdfs-shared
  - kerberos-shared
  - hive-shared
  - sso (currently sso is an user provided service so it is deployed in a slightly different way. The tokenKey service is deployed under uaa.<env_addr>/token_key)

For your curiosity, the script will perform following operations - you don't need to do it manually:
```bash
cf create-service hdfs shared hdfs-shared
cf create-service kerberos shared kerberos-shared
cf create-service hive shared hive-shared
cf cups sso -p '{"tokenKey": "http://uaa.<env_addr>/token_key"}'
```
Then we should have following services : 
```bash
user@host:~/git/hdfs-hive-demo$ cf s
Getting services in org demoorg / space dev as admin...
OK

name              service         plan     bound apps        last operation   
hdfs-shared       hdfs            shared   hdfs-hive-demo   create succeeded   
kerberos-shared   kerberos        shared   hdfs-hive-demo   create succeeded   
sso               user-provided            hdfs-oauth-test      
hive-shared       hive            shared   hdfs-hive-demo    create succeeded
```

Now we can compile and push the app : 
```bash
mvn clean package
cf push
```
Check : 
```bash
user@host:~/git/hdfs-hive-demo$ cf a
Getting apps in org demoorg / space dev as admin...
OK

name              requested state   instances   memory   disk   urls   
hdfs-hive-demo   started           1/1         512M     1G     hdfs-hive-demo.daily-krb.gotapaas.eu   
```

3. Run ./hdfs_demo.sh interactive script
  - A directory called directory_name will be created on hdfs 
  - A file inside directory_name will be created
  
Script execution:
```bash
user@host:~/git/hdfs-hive-demo$ ./hdfs_demo.sh
please enter directory name to create>demo_folder
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   238  100   238    0     0    171      0  0:00:01  0:00:01 --:--:--   171
{
  "directory": true,
  "hdfsPath": "hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/4da271bb-34af-4638-83ba-31f93607305f/demo_folder",
  "url": "http://hdfs-hive-demo.daily-krb.gotapaas.eu/rest/directory/demo_folder"
}
 --- directory hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/4da271bb-34af-4638-83ba-31f93607305f/demo_folder created ---
please enter file name>demo_folder/sample.csv
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   436  100   256  100   180    546    384 --:--:-- --:--:-- --:--:--   545
{
  "directory": false,
  "hdfsPath": "hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/4da271bb-34af-4638-83ba-31f93607305f/demo_folder/sample.csv",
  "url": "http://hdfs-hive-demo.daily-krb.gotapaas.eu/rest/file/demo_folder/sample.csv"
}
 --- file hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/4da271bb-34af-4638-83ba-31f93607305f/demo_folder/sample.csv created ---
```

4. Run ./hive_demo.sh interactive script
 (Please note that due to current TAP permission limitations, for this part of demo it's recommended to use a dataset uploaded to Data Catalog) 
  - An external hive table will be created (you will be prompted for hdfs directory, CSV file containing header, and table name)
  - All data will be fetched from table and printed to the console
  
Script execution:

```bash
user@host:~/git/hdfs-hive-demo$ ./hive_demo.sh
Please enter hdfs path to directory of the dataset>hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/f980d107-b2cc-4022-8f02-c7f4a8f9f0bf/f6901116-968a-4e78-b8e8-7d116424cc5c/
Please enter hdfs path to file with header>hdfs://nameservice1/org/7ae754dc-90e4-41b8-8e53-d0effd076ac7/brokers/userspace/f980d107-b2cc-4022-8f02-c7f4a8f9f0bf/f6901116-968a-4e78-b8e8-7d116424cc5c/000000_1
Please enter hive table name>table1
Please enter hive table column name>sepal_length
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100    86  100    86    0     0     95      0 --:--:-- --:--:-- --:--:--    95
{
  "url": "http://hdfs-hive-demo.daily-krb.gotapaas.eu/rest/hive/table1",
  "name": "table1"
}
Selecting from table:
sepal_length sepal_width petal_length petal_width species
5.1 3.5 1.4 0.2 setosa
4.9 3 1.4 0.2 setosa
4.7 3.2 1.3 0.2 setosa
4.6 3.1 1.5 0.2 setosa
5 3.6 1.4 0.2 setosa
5.4 3.9 1.7 0.4 setosa
4.6 3.4 1.4 0.3 setosa
5 3.4 1.5 0.2 setosa
4.4 2.9 1.4 0.2 setosa
4.9 3.1 1.5 0.1 setosa
5.4 3.7 1.5 0.2 setosa
4.8 3.4 1.6 0.2 setosa
4.8 3 1.4 0.1 setosa
4.3 3 1.1 0.1 setosa
5.8 4 1.2 0.2 setosa
...
Selecting column from table:
sepal_length
5.1 
4.9 
4.7 
4.6 
5
5.4 
4.6 
5
4.4 
4.9 
5.4 
4.8 
4.8 
4.3 
5.8 
...
Deleting table:
Table was deleted.
Selecting again from table:
{"timestamp":1467626961584,"status":500,"error":"Internal Server Error","exception":"java.sql.SQLException",
"message":"Error while compiling statement: FAILED: SemanticException [Error 10001]: Line 1:14 Table not found 'table1'","path":"/rest/hive/table1"}
```

This is the end of our demo scenario.


Please refer to HdfsService and HiveService classes.

In particular, Hadoop FileSystem object is created in following line:

 ```java
FileSystem fs = Hdfs.newInstance().createFileSystem(OauthUtils.getJwtToken());

```

Hive Connection is created by line:
```java
Connection hiveConenction = Hive.newInstance().getConnection(OauthUtils.getJwtToken())
```

Where OauthUtils.getJwtToken() returns the user token from request. 
You can obtain the token in the command line using: cf oauth-token 


