package database;

import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlDatabase {
    private static String driver;
    private static String url;
    private static String user;
    private static String password;


    public static Connection getConnection(){
        Connection cn = null;
        try{
            Class.forName(Constant.mysqlDriver);
            cn = DriverManager.getConnection(Constant.mysqlUrl, Constant.mysqlUser, Constant.mysqPassword);

        }catch (Exception e){
            e.printStackTrace();
        }
        return cn;
    }





}
