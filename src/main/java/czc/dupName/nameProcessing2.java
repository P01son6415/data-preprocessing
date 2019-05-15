package czc.dupName;

import com.mysql.cj.util.TestUtils;
import database.MysqlDatabase;
import database.Neo4jDatabase;
import org.apache.http.util.TextUtils;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;

import javax.swing.plaf.nimbus.State;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class nameProcessing2 {
    private static HashMap<String, Integer> partnerMap;

    public static void main(String[] args) {

        final Connection mysqlConnection = MysqlDatabase.getConnection();
        Session sessionAll = Neo4jDatabase.getSession();
        int authorNum = sessionAll.run("MATCH (a:czc_Author) RETURN COUNT(a) AS count").next().get("count").asInt();
        sessionAll.close();

        int currentId = 0;
        String currentAuthor = null;
        while (currentId++ < authorNum) {   //根据id进行处理
            try {
                Session session = Neo4jDatabase.getSession();
                StatementResult idResult = session.run("MATCH (a:czc_Author) where a.authorId = " + currentId + " return a.author_name AS name");
                if (!idResult.hasNext()) continue;   //如果节点不存在就跳过
                currentAuthor = idResult.next().get("name").asString();

                //查询重名情况返回Id
                StatementResult dupResult = session.run("MATCH (a:czc_Author {author_name:\"" + currentAuthor + "\"}) "
                        + " return a.authorId");

                //存储重名的Id
                ArrayList<Integer> dupAuthorId = new ArrayList<Integer>();
                while (dupResult.hasNext()) {
                    int dupId = dupResult.next().values().get(0).asInt();
                    dupAuthorId.add(dupId);
                }
                int len = dupAuthorId.size();

                if (len == 1) continue;  //没有重名就跳过
                if (!(dupAuthorId.indexOf(currentId) == 0)) continue;  //重名已处理就跳过

                 
                for (int i = 0; i < len; i++) {
                    for (int j = i; j < len; j++) {
                        if (compareAuthor(dupAuthorId.get(i), dupAuthorId.get(j))) {
                            //TODO:合并重名

                        }
                    }
                }
                session.close();
            } catch (Exception e) {
                System.out.println("数据处理出错 " + "ID: " + currentId + " Author: " + currentAuthor);
            }
        }
    }

    public static boolean compareAuthor(int firstId, int dupId) {
        Session comSession = Neo4jDatabase.getSession();
        Boolean isDup = false;
        //TODO:比较合作关系
        //查询原始作者的合作关系
        StatementResult partnerResult = comSession.run("MATCH (a:czc_Author {authorId:" + firstId
                + "})-[:WRITE]->()<-[:WRITE]-(b:czc_Author) RETURN b.author_name, b.authorId");
        if (partnerResult.hasNext()) {   //如果有合作关系
            partnerMap = new HashMap<String, Integer>();
            while (partnerResult.hasNext()) {   //存储当前作者的合作关系
                Record record = partnerResult.next();
                partnerMap.put(record.values().get(0).asString(), record.values().get(1).asInt());
            }
        }
        //查询重名作者的合作关系
        StatementResult dupPartner = comSession.run("MATCH (a:czc_Author {authorId:" + dupId
                + "})-[:WRITE]->()<-[:WRITE]-(b:czc_Author) RETURN b.author_name AS name");
        while (dupPartner.hasNext()) {
            String str = dupPartner.next().get("name").asString();
            if (partnerMap.containsKey(str)) {   // 只要 有相同的合作关系
                isDup = true;
                break;
            }
        }

        //TODO:比较作者机构
        String firstOrg = comSession.run("MATCH (a:czc_Author {authorId:" + firstId
                + "})-[:BELONG]->(o) RETURN o.org_name AS org").next().get("org").asString();
        String dupOrg = comSession.run("MATCH (a:czc_Author {authorId:" + dupId
                + "})-[:BELONG]->(o) RETURN o.org_name AS org").next().get("org").asString();
        if (firstOrg.equals(dupOrg)) isDup = true;

        //TODO:判断二者的研究领域（word2vec计算相关度，根据提前确定的阈值进行筛查）


        comSession.close();
        return isDup;
    }
}
