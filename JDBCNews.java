package com.actimize.automation.redeye.tests;

import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JDBCNews {

    public Properties config = null;
    @Test
    public static void runJDBC() throws IOException, InterruptedException {
        int countfromdb=0;
        Properties config = new Properties();
        FileInputStream objfile = new FileInputStream("src/main/resources/redeye/objectRepositories/config.properties");
        config.load(objfile);
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con= DriverManager.getConnection(
                    //replace with object repository-------------
                    config.getProperty("HOSTURL"),config.getProperty("USER"), config.getProperty("PASSWORD"));
                    //replace with object repository-------------
//here newsserverdb is database name, root is username and password
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery("SELECT count(* ) FROM newsserverdb.newsdata where status= 'SENT_TO_ACTIVE_CLIENTS' and  NewsDate >current_date()-1;");
            while (rs.next()) {
                System.out.println(rs.getString(1));
                countfromdb=Integer.parseInt(rs.getString(1));
            }
            con.close();
            Assert.assertTrue("Count is not greater than zero ",countfromdb>0);
            Assert.assertTrue("Count is less than average count from ravenpack",countfromdb>Integer.parseInt(config.getProperty("COUNT")));

        }catch(Exception e){ System.out.println(e);}
    }
}
