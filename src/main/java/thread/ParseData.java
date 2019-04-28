package thread;

import database.Neo4jDatabase;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ParseData extends Thread{

    private Thread t;
    private String threadName;

    private static IdManager idManager = new IdManager();
    private static MysqlManager mysqlManager = new MysqlManager();

    private static Logger logger = Logger.getLogger(ParseData.class);

    private int totalThread = 0;
    static ConcurrentHashMap<Integer,Integer> activeThread = new ConcurrentHashMap<Integer, Integer>();

    ParseData(String name) {
        threadName = name;
        System.out.println("Creating " +  threadName );
    }

    public void run()  {
        System.out.println("Running " +  threadName );
        activeThread.put(this.hashCode(),0);
        Session session = Neo4jDatabase.getSession();
        try {
            ResultSet mResult = mysqlManager.getBatch();
            while (mResult.next()){
                try {
                    /*
                        提取实体
                     */
                    String author = mResult.getString("Author");
                    String paper = mResult.getString("Title").replace("\"", "\\\"");
                    String org = mResult.getString("Organ").replace("\"", "\\\"");
                    String year = mResult.getString("Year");
                    String journal = mResult.getString("JournalName");
                    String keyword = mResult.getString("Keyword");

                    String authorOrgan = mResult.getString("AuthorOrgan");
                    StatementResult paperResult;
                    StatementResult orgResult;
                    StatementResult journalResult;

                    int paperId = idManager.getPaperId();


                    /*
                        添加实体及关系
                     */

                    //添加paper实体

                    //如果paper为空则跳过当前行数据
                    if (TextUtils.isEmpty(paper)) {
                        continue;
                    }
                    //查询paper实体是否已经在图数据库中
                    paperResult = session.run("MATCH (p:czc_Paper) WHERE p.paper_name =\"" + paper.trim() + "\" RETURN p");
                    //如果存在则跳过当前行数据
                    if (paperResult.hasNext()) {
                        continue;
                    }
                    //检查作者是否为空
                    HashMap<String, Integer> authorMap = new HashMap<String, Integer>();
                    if (!TextUtils.isEmpty(author)) {
                        //添加paper实体
                        session.run("CREATE (p:czc_Paper {paperId: " + paperId + ", paper_name: \"" + paper.trim()
                                + "\", paper_class: \"" + mResult.getString("Class") + "\"})");
                        //年份不为空时
                        if (!TextUtils.isEmpty(year)) {
                            //添加paper-year关系
                            session.run("MATCH (p:czc_Paper),(y:czc_Year) WHERE p.paperId =" + paperId
                                    + " AND y.year =\"" + year.trim() + "\" CREATE (p)-[r:TIME]->(y)");
                        }

                        //添加author实体
                        //TODO: 使用HashMap记录<Author:ID>
                        String[] authorArray = author.split(";");
                        for (int i = 0; i < authorArray.length; i++) {
                            if (!TextUtils.isEmpty(authorArray[i].trim())) {  //判空（必要）
                                int authorId = idManager.getAuthorId();
                                session.run("CREATE (p:czc_Author {authorId: " + authorId
                                        + ", author_name: \"" + authorArray[i].trim() + "\"})");
                                authorMap.put(authorArray[i].trim(), authorId);
                                //添加author-paper关系
                                session.run("MATCH (a:czc_Author),(p:czc_Paper) WHERE a.authorId =" +
                                        authorId + " AND p.paperId =" + paperId
                                        + " CREATE (a)-[r:WRITE]->(p)");
                            }
                        }
                    } else {
                        //System.out.println("缺少作者的论文：" + paper);
                        continue;
                    }

                    //添加organ实体
                    if (!TextUtils.isEmpty(org)) {  //判空
                        String[] orgArray = org.split(";");
                        for (int i = 0; i < orgArray.length; i++) {
                            //查询organization实体是否已经存在
                            orgResult = session.run("MATCH (o:czc_Organ) WHERE o.org_name = \""
                                    + orgArray[i].trim() + "\" RETURN o");
                            //不存在则添加organization实体
                            if (!orgResult.hasNext()) {  //去重
                                int orgId = idManager.getOrgId();
                                session.run("CREATE (o:czc_Organ {orgId: " + orgId
                                        + ", org_name: \"" + orgArray[i].trim() + "\"})");
                            }
                        }
                    }


                    //添加journal实体
                    if (!TextUtils.isEmpty(journal)) {  //判空
                        journalResult = session.run("MATCH (j:czc_Journal) WHERE j.journal_name = \""
                                + journal.trim() + "\" RETURN j");
                        if (!journalResult.hasNext()) {  //去重
                            int journalId = idManager.getJournalId();
                            session.run("CREATE (j:czc_Journal {journalId: " + journalId
                                    + ", journal_name: \"" + journal.trim() + "\"})");
                        }
                        //添加paper-journal关系
                        session.run("MATCH (p:czc_Paper),(j:czc_Journal) WHERE p.paperId =" +
                                paperId + " AND j.journal_name =\"" + journal.trim()
                                + "\" CREATE (p)-[r:BELONG]->(j)");
                    }

                    //添加keyword实体
                    if (!TextUtils.isEmpty(keyword)) {  //判空
                        String[] keywordArray = keyword.split(";");
                        for (int i = 0; i < keywordArray.length; i++) {
                            if (!TextUtils.isEmpty(keywordArray[i].trim())) {  //判空
                                StatementResult keyword_result = session.run("MATCH (k:czc_Keyword) WHERE k.keyword_name = \"" +
                                        keywordArray[i].trim() + "\" RETURN k");
                                if (!keyword_result.hasNext()) {  //去重
                                    int keywordId = idManager.getKeywordId();
                                    session.run("CREATE (k:czc_Keyword {keywordId: " + keywordId
                                            + ", keyword_name: \"" + keywordArray[i].trim() + "\"})");
                                }
                                //添加paper-keyword关系
                                session.run("MATCH (p:czc_Paper),(k:czc_Keyword) WHERE p.paperId =" + paperId +
                                        " AND k.keyword_name =\"" + keywordArray[i].trim()
                                        + "\" CREATE (p)-[r:INVOLVED]->(k)");
                            }
                        }
                    }

                    //添加author-organ关系
                    //TODO:从HashMap中取作者ID，与Organ建立关系
                    if (!TextUtils.isEmpty(authorOrgan)) {
                        String a = null;
                        String o = null;
                        String[] aoArray = authorOrgan.split(";");
                        for (int i = 0; i < aoArray.length; i++) {
                            if (aoArray[i].contains("[")) {
                                a = aoArray[i].split("\\[")[0].trim();
                                o = aoArray[i].split("\\[")[1].trim()
                                        .replace("]", "")
                                        .replace("\"", "\\\"");
                                session.run("MATCH (a:czc_Author),(o:czc_Organ) WHERE a.authorId = " +
                                        authorMap.get(a) + " AND o.org_name = \"" + o
                                        + "\" CREATE (a)-[r:BELONG]->(o)");
                            }
                        }
                    }
                    //endtime = System.nanoTime();
                    //logger.info("statge5: " + (endtime-begintime)/1000 );
                } catch (Exception e) {
                    logger.warn("处理数据时出错");
                }
            }
        } catch (SQLException e){}
        finally {
            session.close();
        }
        activeThread.remove(this.hashCode());
        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}

class TestThread {

    static MysqlManager mysqlManager = new MysqlManager();
    public static void main(String args[]) {

        while (true){
            if(ParseData.activeThread.size()<12) {
                ParseData T1 = new ParseData("T"+mysqlManager.getRound());
                T1.start();
            }

            try {
                Thread.sleep(2000);
            }catch (Exception e){}
            if(mysqlManager.getRound()>=mysqlManager.getAllRounds()){
                break;
            }
        }

    }
}