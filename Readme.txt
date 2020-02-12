该工程为H3C DataEngine产品中Hive组件提供样例代码，而且区分kerberos环境和非kerberos环境。该工程的目录结构介绍：

HiveExampleCode/
  |Readme.txt                      --介绍文档
  |hive-example-normal/       --非Kerberos环境下样例代码
     |pom.xml                      --pom文件
     |src/	                   --hive增删查改样例代码
  |hive-example-security/     --Kerberos环境下样例代码
     |pom.xml			   --pom文件
     |src/	                   --hive增删查改样例代码
     
修改示例中url等变量，可以直接在IDEA上执行，或者编译打包生成带依赖的Jar包，在客户端服务器上执行如下命令：
java -jar hivedemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.h3c.hive.SimpleExample
