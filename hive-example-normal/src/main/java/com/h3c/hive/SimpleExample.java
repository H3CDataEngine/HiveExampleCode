package com.h3c.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleExample {
    private static String driveName = "org.apache.hive.jdbc.HiveDriver" ;
    private static String url = "jdbc:hive2://101.8.202.1:10010/default";
    private static String user = "hive";
    private static String passwd = null;
    private static String sql = "";
    private static ResultSet res;
    private static ResultSetMetaData m;

    public static void main(String[] args){
        Connection con = null;
        Statement stm = null;

        try{
            con = getConnection();
            stm = con.createStatement();

            String tableName = "stu2";
            dropTable(stm,tableName);
            createTable(stm,tableName);
            showTables(stm, tableName);
            describeTale(stm, tableName);
            selectData(stm, tableName);
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
        System.out.println("执行 show tables的运行结果如下：");
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
        System.out.println("执行 describe tables的运行结果如下：");
        while(res.next()){
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    private static void selectData(Statement stm, String tableName)throws SQLException{
        sql = "select * from "+tableName;
        System.out.println("Running:"+sql);
        res = stm.executeQuery(sql);
        System.out.println("执行 select * 的运行结果如下：");
        while(res.next()){
            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t"
                    + res.getString(3) + "\t" + res.getString(4));
        }
    }
}
