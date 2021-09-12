package com.runnertest.file;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
import java.util.Map;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class Code {

    public Properties config = null;
    static Logger log = Logger.getLogger(Code.class.getName());

    @Test
    public static void runJDBC() throws IOException, InterruptedException, ParseException, ClassNotFoundException, SQLException {
        String path= "src/main/resources/logfile.txt";
        Properties config = new Properties();
        FileInputStream objfile = new FileInputStream("src/main/resources/config.properties");
        config.load(objfile);
        JSONParser jsonParser = new JSONParser();
        FileReader reader = new FileReader(path);
        Object obj= jsonParser.parse(reader);
        JSONArray  logpbj= (JSONArray) obj;
        ArrayList<Object> arlist = new ArrayList<Object>();
        String [] strarr = new String[0];
        String [] keyval = new String[0];
        String tsused=null;
        String previouslogged=null;
        String nstate=null;
        String ntsused=null;
        String npreviouslogged=null;
        String mapinsertid=null;
        int mapinserts=0  ;
        String mapinserttype=null;
        String mapinserthost=null;
        Boolean alert=true;
        Connection con = null;
        Statement stmt = null;
        int result = 0;
        double tsdiff=0;
        Map<String,String> mp= new HashMap<String,String>();
        ArrayList listmp= new ArrayList();
        for (int i =0; i <=logpbj.size()-1;i++)
        {
            String jsonString = logpbj.get(i).toString();
            strarr = jsonString.split(",");

            for (String name : strarr)
            {
                String newName = name.replace("{", "").replace("}", "");
                if ( newName.contains("STARTED"))
                    {
                        keyval= newName.split(":");
                        nstate= keyval[1];
                        mp.put(keyval[0],keyval[1] );
                    }
                if ( newName.contains("state"))
                {
                    keyval= newName.split(":");
                    mp.put(keyval[0],keyval[1] );
                }
                if ( newName.contains("host"))
                {
                    keyval= newName.split(":");
                    mp.put(keyval[0],keyval[1] );
                }
                if ( newName.contains("type"))
                {
                    keyval= newName.split(":");
                    mp.put(keyval[0],keyval[1] );
                }
                 if ( newName.contains("timestamp")) {
                     keyval = newName.split(":");
                     ntsused = keyval[1];
                     mp.put(keyval[0],keyval[1] );
                     if (npreviouslogged.equals(previouslogged))
                     {
                        tsdiff= Double.parseDouble(keyval[1])-Double.parseDouble(tsused);
                     }
                     else
                     {continue;}
                     mp.put(keyval[0],keyval[1] );
                     if ( tsdiff>=4)
                     {
                         log.info("This alert is needs to be flagged");
                         log.info("Mp value"+mp);
                         mapinsertid=mp.get("id");
                         mapinserts=(int)(tsdiff);
                         mapinserttype=mp.get("type");
                         mapinserthost=mp.get("host");
                         for (Map.Entry<String,String> entry : mp.entrySet())
                             if (entry.getKey().contains("id"))
                             {
                                 mapinsertid=entry.getValue();
                                 log.info("idinside"+mapinsertid);
                             }
                          else
                         if (entry.getKey().contains("type"))
                         {
                             mapinserttype=entry.getValue()
                             ;
                         }
                         else
                         if (entry.getKey().contains("host"))
                         {
                             mapinserthost=entry.getValue()
                             ;
                         }
                         //Display map values
                         log.info(mapinsertid+mapinserts+mapinserttype+mapinserthost);
                         try {
                             Class.forName("org.hsqldb.jdbc.JDBCDriver");
                             con = DriverManager.getConnection( config.getProperty("URL"), config.getProperty("USER"), "");
                             stmt = con.createStatement();
                             log.info("insert");
                             result = stmt.executeUpdate("INSERT INTO event VALUES(mapinsertid,mapinserts,mapinserttype,mapinserthost,alert,");
                             con.commit();
                         }catch (Exception e) {
                             e.printStackTrace(System.out);
                         }
                         log.info(result+" rows effected");
                     }
                     mp.put(keyval[0],keyval[1] );
                 }
                if ( newName.contains("id"))
                {
                    keyval= newName.split(":");
                    npreviouslogged= keyval[1];
                    mp.put(keyval[0],keyval[1] );
                }
                listmp.add(mp);
            }
            try {
                Class.forName("org.hsqldb.jdbc.JDBCDriver");
                con = DriverManager.getConnection(config.getProperty("URL"), config.getProperty("USER"), "");
                stmt = con.createStatement();

                result = stmt.executeUpdate("CREATE TABLE event IF NOT EXISTS ( " +
                                "eventid INT NOT NULL, duration INT NOT NULL        ,type VARCHAR(20) , hostname VARCHAR(20)," +
                        "ALERT BOOLEAN DEFAULT FALSE NOT NULL;");
                con.commit();

            }  catch (Exception e) {
                e.printStackTrace(System.out);
            }
            log.info("Table created successfully");


            previouslogged=npreviouslogged;
            tsused=ntsused;

        }
        log.info("listmp"+listmp);
    }

}
