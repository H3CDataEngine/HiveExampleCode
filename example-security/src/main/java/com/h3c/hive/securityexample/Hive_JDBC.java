package com.h3c.hive.securityexample;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
/**
 * Created by z15532 on 2017/11/7.
 */

/*
     注意创建连接，进行数据库相关操作之前，要在机器上打开hiveserver2
     在外部Hive命令中，可以通过下面的方式启动HiveServer2
	 $HIVE_HOME/bin/hive --service hiveserver2
 */

public class Hive_JDBC {
    private static String driveName = "org.apache.hive.jdbc.HiveDriver" ;
    //开启kerberos的集群环境，请根据实际环境修改配置
    private static String url = "jdbc:hive2://101.8.202.1:10010/default;principal=hive/node1.hde.h3c.com@HDE.H3C.COM";

    private static String user = "hive";
    private static String passwd = "hive";
    private static String sql = "";
    private static ResultSet res;
    private static ResultSetMetaData m = null;//获取列信息

    public static void main(String[] args){
        Connection con = null;
        Statement stm = null;

        try{
            con = getConnection();//创建连接
            stm = con.createStatement();

            String tableName = "stu2";
            dropTable(stm,tableName);//若表存在则先删除表
            createTable(stm,tableName);//若表不存在则新建
            showTables(stm, tableName);//查看当前数据库下的所有表
            describeTale(stm, tableName);//查看创建的表的表结构
//            loadData(stm, tableName);//往新建的表装载数据
            selectData(stm, tableName);//全表查询
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            System.out.println(driveName + " not found! ");
            System.out.println(e.getMessage());
        }catch (SQLException e1){
            e1.printStackTrace();
            System.out.println("connection error! ");
            System.out.println(e1.getMessage());
        }finally {
            try{
                if(res!=null){
                    res.close();
                    res=null;
                }
                if(stm!=null){
                    stm.close();
                    stm=null;
                }
                if(con!=null){
                    con.close();
                    con=null;
                }
            }catch (SQLException e2){
                e2.printStackTrace();
                System.out.println("close connection or statement error! " );
                System.out.println(e2.getMessage());
            }
        }
    }

    private static Connection getConnection() throws ClassNotFoundException,SQLException{
        //Kerberos身份认证，请将集群中krb5.conf及keytab文件放入该工程resource文件夹中
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        //获取krb5.conf配置
        if(System.getProperty("os.name").toLowerCase().startsWith("win")) {
            System.setProperty("java.security.krb5.conf", Hive_JDBC.class.getClassLoader().getResource("./krb5.conf").getPath());
        }
        //进行身份认证
        try{
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hive/node1.hde.h3c.com@HDE.H3C.COM",Hive_JDBC.class.getClassLoader().getResource("./hive.service.keytab").getPath());
        }catch(IOException e1){
            e1.printStackTrace();
        }

        Class.forName(driveName);
        Connection con = DriverManager.getConnection(url,user,passwd);
        System.out.println("connection success!");
        return con;
    }

    private static void dropTable(Statement stm, String tableName)throws SQLException{
        sql = "drop table if exists "+tableName;
        System.out.println("Running:"+sql);
        stm.executeUpdate(sql);
    }

    private static void createTable(Statement stm, String tableName)throws SQLException{
        sql = "create table if not exists "+tableName+" (stuid string, name string, sex string, age int)"
                +" row format delimited fields terminated by '\t'"
                + " lines terminated by '\n' "
                + " stored as textfile";
        System.out.println("Running:"+sql);
        stm.executeUpdate(sql);
    }

    private static void showTables(Statement stm, String tableName)throws SQLException{
        sql = "show tables in default";
        System.out.println("Running:"+sql);
        res = stm.executeQuery(sql);
        System.out.println("执行 show tables 的运行结果如下：");
        m=res.getMetaData();
        int columns=m.getColumnCount();
        while(res.next())
        {
            for(int i=1;i<=columns;i++)
            {
                System.out.print(res.getString(i)+"\t\t");
            }
            System.out.println();
        }
    }

    private static void describeTale(Statement stm, String tableName)throws SQLException{
        sql = "describe " + tableName;
        System.out.println("Running:"+sql);
        res = stm.executeQuery(sql);
        System.out.println("执行 describe table 的运行结果如下：");
        printInfo(res);
    }
    /*

    private static void loadData(Statement stm, String tableName)throws SQLException{
        String filePath = "\"/tmp/stu1info\""; //要装载的数据源文件的路径
        sql = "load data local inpath "+filePath+" into table "+tableName; //从本地文件导入数据
        //sql = "load data inpath "+filePath+" into table "+tableName;//从HDFS中的文件导入数据
        System.out.println("Running:"+sql);
        stm.executeUpdate(sql);
    }
    */

    private static void selectData(Statement stm, String tableName)throws SQLException{
        sql = "select * from "+tableName;
        System.out.println("Running:"+sql);
        res = stm.executeQuery(sql);
        System.out.println("执行 select *  的运行结果如下：");
        printInfo(res);
    }

    private static void printInfo (ResultSet res)throws SQLException{
        m=res.getMetaData();
        int columns=m.getColumnCount();

        //显示列,表格的表头
        for(int i=1;i<=columns;i++)
        {
            System.out.print(m.getColumnName(i)+"\t\t");
        }
        System.out.println();

        //显示表格内容
        while(res.next())
        {
            for(int i=1;i<=columns;i++)
            {
                System.out.print(res.getString(i)+"\t\t");
            }
            System.out.println();
        }
    }

}

