package keyword;

import database.Neo4jDatabase;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import thread.IdManager;
import thread.MysqlManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public class GetKeyword extends Thread{


    private Thread t;
    private String threadName;

    private static IdManager idManager = new IdManager();
    private static MysqlManager mysqlManager = new MysqlManager();

    private static Logger logger = Logger.getLogger(GetKeyword.class);

    private int totalThread = 0;
    static ConcurrentHashMap<Integer,Integer> activeThread = new ConcurrentHashMap<Integer, Integer>();

    GetKeyword(String name) {
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
                    String keyword = mResult.getString("Keyword");




                    //添加keyword实体
                    if (!TextUtils.isEmpty(keyword)) {  //判空
                        String[] keywordArray = keyword.split(";");
                        for (int i = 0; i < keywordArray.length; i++) {
                            if (!TextUtils.isEmpty(keywordArray[i].trim())) {  //判空
                                StatementResult keyword_result = session.run("MATCH (k:vector_Keyword) WHERE k.keyword_name = \"" +
                                        keywordArray[i].trim() + "\" RETURN k");
                                if (!keyword_result.hasNext()) {  //去重
                                    int keywordId = idManager.getKeywordId();
                                    session.run("CREATE (k:vector_Keyword {keywordId: " + keywordId
                                            + ", keyword_name: \"" + keywordArray[i].trim() + "\"})");
                                }

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
            t = new Thread(this, threadName);
            t.start ();
        }
    }
}

class TestThread {

    static MysqlManager mysqlManager = new MysqlManager();
    public static void main(String args[]) {

        while (true){
            if(GetKeyword.activeThread.size()<12) {
                GetKeyword T1 = new GetKeyword("T"+mysqlManager.getRound());
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