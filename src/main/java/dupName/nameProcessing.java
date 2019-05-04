package dupName;

import database.MysqlDatabase;
import database.Neo4jDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import sun.jvm.hotspot.runtime.solaris_sparc.SolarisSPARCJavaThreadPDAccess;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.Semaphore;

public class nameProcessing {
    public static void main(String[] args) {
        final Connection mysqlConnection = MysqlDatabase.getConnection();
        Session sessionAll = Neo4jDatabase.getSession();

//        StatementResult result = session.run("MATCH (a:czc_Author) return a");  //这个方法在数据库试了一下运行太慢，不考虑
        int authorNum = sessionAll.run("MATCH (a:czc_Author) RETURN COUNT(a) AS count").next().get("count").asInt();
        sessionAll.close();
        int authorId = 0;
        while (authorId++ < authorNum) {
            Session session = Neo4jDatabase.getSession();
            StatementResult result = session.run("MATCH (a:czc_Author) WHERE a.authorId = " + authorId
                    + " RETURN a.author_name AS name");
            if (result.hasNext()) {  //如果这个ID没有被删除
                String authorName = result.next().get("name").asString();
                StatementResult nameResult = session.run("MATCH (a:czc_Author) WHERE a.author_name = \"" + authorName + "\" RETURN a");  //查询所有重名
                while (nameResult.hasNext()){
                    nameResult.next();
                    // TODO
                }
            }
        }
    }
}
