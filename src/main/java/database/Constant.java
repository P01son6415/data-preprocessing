package database;

public class Constant {

    public static String mysqlDriver = "com.mysql.cj.jdbc.Driver";
    //local
    public static String mysqlUrlKG = "jdbc:mysql://192.168.229.151:3306/kg?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8";
    public static String mysqlUrlKejso = "jdbc:mysql://192.168.229.151:3306/kejso?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8";
    public static String mysqlUser = "user1012";
    public static String mysqPassword = "123456";

    public static String getNeo4Driver = "bolt://192.168.229.204:7687";
    public static String neo4jUrl = "jdbc:neo4j:http://192.168.229.204:7474";
    public static String neo4juser = "neo4j";
    public static String neo4jpassword = "yb1135";
}
