package czc.dupName;

import database.MysqlDatabase;
import database.Neo4jDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;

import javax.swing.plaf.nimbus.State;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

public class nameProcessing {
    public static void main(String[] args) {
        HashMap<String, Integer> partnerMap;

        final Connection mysqlConnection = MysqlDatabase.getConnection();
        Session sessionAll = Neo4jDatabase.getSession();
//        StatementResult result = session.run("MATCH (a:czc_Author) return a");  //这个方法在数据库试了一下运行太慢，不考虑
        int authorNum = sessionAll.run("MATCH (a:czc_Author) RETURN COUNT(a) AS count").next().get("count").asInt();
        sessionAll.close();

        int currentId = 0;
        String currentAuthor;
        while (currentId++ < authorNum) {   //根据id进行处理
            Session session = Neo4jDatabase.getSession();
            StatementResult idResult = session.run("MATCH (a:czc_Author) where a.authorId = " + currentId + " return a.author_name AS name");
            if (!idResult.hasNext()) continue;   //如果节点不存在就跳过

            currentAuthor = idResult.next().get("name").asString();
            //查询重名情况返回Id
            StatementResult dupResult = session.run("MATCH (a:czc_Author {author_name:\"" + currentAuthor + "\"}) " +
                    "where a.authorId<>" + currentId + " return a.authorId");
            if (!dupResult.hasNext()) continue;   //如果没有重名就跳过

            //查询当前作者的合作关系
            StatementResult partnerResult = session.run("MATCH (a:czc_Author {authorId:" + currentId + "})-[:WRITE]->()<-[:WRITE]-(b:czc_Author) " +
                    "RETURN b.author_name, b.authorId");
            if (partnerResult.hasNext()) {   //如果有合作关系
                partnerMap = new HashMap<String, Integer>();
                while (partnerResult.hasNext()) {   //存储当前作者的合作关系
                    Record record = partnerResult.next();
                    partnerMap.put(record.values().get(0).asString(), record.values().get(1).asInt());
                }
            } else continue;   //如果没有合作关系就跳过（有待更改）

            // 判断重名是否和当前ID的名字有相同的合作关系，删除重名及其关系并重新建立关系
            while (dupResult.hasNext()) {
                int dupId = dupResult.next().values().get(0).asInt();
                System.out.println(dupId);

                StatementResult dupPartnerResult = session.run("MATCH (a:czc_Author {authorId:" + dupId + "})-[:WRITE]->()<-[:WRITE]-(b:czc_Author) " +
                        "RETURN b.author_name AS name");
                while (dupPartnerResult.hasNext()) {
                    String str = dupPartnerResult.next().get("name").asString();
                    if (partnerMap.containsKey(str)) {  //如果有相同的合作关系
                        //TODO:判断二者的研究领域（word2vec计算相关度，根据提前确定的阈值进行筛查）
                        //TODO:对二者的机构进行检查
                    }
                }
            }
        }
    }
}
