package database;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.Connection;
import java.sql.DriverManager;

public class Neo4jDatabase {
    public static Connection getConnection(){
        Connection cn = null;
        try{
            cn = DriverManager.getConnection(Constant.neo4jUrl,Constant.neo4juser,Constant.neo4jpassword);
        }catch (Exception e){
            e.printStackTrace();
        }
        return cn;
    }
    public static Session getSession() {
        Driver driver = GraphDatabase.driver(Constant.getNeo4Driver,
                AuthTokens.basic(Constant.neo4juser, Constant.neo4jpassword));
        return driver.session();
    }
}
