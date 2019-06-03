package processing;


import database.Constant;
import database.MysqlDatabase;
import database.Neo4jRest;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.StatementResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class MysqlNeo4jRest {
    private static Logger logger = Logger.getLogger(MysqlNeo4jRest.class);

    public static void main(String[] args) throws SQLException {
        Connection mysqlConnection = MysqlDatabase.getConnection(Constant.mysqlUrlKG);

        /*
            获得表格总行数，计算循环次数
         */
        ResultSet result = mysqlConnection.createStatement().executeQuery("SELECT count(*) as count FROM ArticleInfo_2010");
        result.next();
        int row = result.getInt("count");
        int allRounds = (int) (row / 1000);
        logger.info("获取到 " + row + " 条数据");

        /*
            初始化主键（ID）
         */
        int authorId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Author) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        int paperId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Paper) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        int orgId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Organ) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        int journalId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Journal) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        int keywordId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Keyword) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());

        /*
            每次循环处理1000条数据
         */
        for (int round = 0; round < allRounds; round++) {
            PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_2010 limit ?,1000");
            mysqlPs.setInt(1, 1000 * round);
            ResultSet mResult = mysqlPs.executeQuery();
            int line = 0;
            //依次从每行数据中提取实体、建立关系


            while (mResult.next()) {
                line++;
                if (line % 10 == 0) {
                    logger.info("当前已完成第 " + (round + 1) + " 轮，" + (line / 1000.0) * 100 + "%");
                }
                try {
                    /*
                        提取实体
                     */
                    String author = mResult.getString("Author");
                    String paper = mResult.getString("Title").replace("\"", "\\\"").replace("\'", "\\\'");
                    String org = mResult.getString("Organ").replace("\"", "\\\"").replace("\'", "\\\'");
                    String year = mResult.getString("Year");
                    String journal = mResult.getString("JournalName");
                    String keyword = mResult.getString("Keyword");

                    String authorOrgan = mResult.getString("AuthorOrgan");
                    StatementResult paperResult;
                    StatementResult orgResult;
                    StatementResult journalResult;

                    /*
                        添加实体及关系
                     */

                    //添加paper实体

                    //如果paper为空则跳过当前行数据
                    if (TextUtils.isEmpty(paper)) {
                        continue;
                    }
                    //查询paper实体是否已经在图数据库中
                    boolean paperExist = Neo4jRest.queryExist("MATCH (p:czc_Paper) WHERE p.paper_name =\'" + paper.trim() + "\' RETURN p");
                    //如果存在则跳过当前行数据
                    if (paperExist) {
                        continue;
                    }
                    //检查作者是否为空
                    HashMap<String, Integer> authorMap = new HashMap<String, Integer>();
                    if (!TextUtils.isEmpty(author)) {
                        paperId++;
                        //添加paper实体
                        Neo4jRest.execute("CREATE (p:czc_Paper {paperId: " + paperId + ", paper_name: \'" + paper.trim()
                                + "\', paper_class: \'" + mResult.getString("Class") + "\'})");
                        //年份不为空时
                        if (!TextUtils.isEmpty(year)) {
                            //添加paper-year关系
                            Neo4jRest.execute("MATCH (p:czc_Paper),(y:czc_Year) WHERE p.paperId =" + paperId
                                    + " AND y.year =\'" + year.trim() + "\' CREATE (p)-[r:TIME]->(y)");
                        }

                        //添加author实体
                        String[] authorArray = author.split(";");
                        for (int i = 0; i < authorArray.length; i++) {
                            if (!TextUtils.isEmpty(authorArray[i].trim())) {  //判空（必要）
                                authorId++;
                                Neo4jRest.execute("CREATE (p:czc_Author {authorId: " + authorId
                                        + ", author_name: \'" + authorArray[i].trim() + "\'})");
                                authorMap.put(authorArray[i].trim(), authorId);
                                //添加author-paper关系
                                Neo4jRest.execute("MATCH (a:czc_Author),(p:czc_Paper) WHERE a.authorId =" +
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
                            boolean organExist = Neo4jRest.queryExist("MATCH (o:czc_Organ) WHERE o.org_name = \'"
                                    + orgArray[i].trim() + "\' RETURN o");
                            //不存在则添加organization实体
                            if (!organExist) {  //去重
                                orgId++;
                                Neo4jRest.execute("CREATE (o:czc_Organ {orgId: " + orgId
                                        + ", org_name: \'" + orgArray[i].trim() + "\'})");
                            }
                        }
                    }


                    //添加journal实体
                    if (!TextUtils.isEmpty(journal)) {  //判空
                        boolean journalExist = Neo4jRest.queryExist("MATCH (j:czc_Journal) WHERE j.journal_name = \'"
                                + journal.trim() + "\' RETURN j");
                        if (!journalExist) {  //去重
                            journalId++;
                            Neo4jRest.execute("CREATE (j:czc_Journal {journalId: " + journalId
                                    + ", journal_name: \'" + journal.trim() + "\'})");
                        }
                        //添加paper-journal关系
                        Neo4jRest.execute("MATCH (p:czc_Paper),(j:czc_Journal) WHERE p.paperId =" +
                                paperId + " AND j.journal_name =\'" + journal.trim()
                                + "\' CREATE (p)-[r:BELONG]->(j)");
                    }

                    //添加keyword实体
                    if (!TextUtils.isEmpty(keyword)) {  //判空
                        String[] keywordArray = keyword.split(";");
                        for (int i = 0; i < keywordArray.length; i++) {
                            if (!TextUtils.isEmpty(keywordArray[i].trim())) {  //判空
                                boolean keywordExist = Neo4jRest.queryExist("MATCH (k:czc_Keyword) WHERE k.keyword_name = \'" +
                                        keywordArray[i].trim() + "\' RETURN k");
                                if (!keywordExist) {  //去重
                                    keywordId++;
                                    Neo4jRest.execute("CREATE (k:czc_Keyword {keywordId: " + keywordId
                                            + ", keyword_name: \'" + keywordArray[i].trim() + "\'})");
                                }
                                //添加paper-keyword关系
                                Neo4jRest.execute("MATCH (p:czc_Paper),(k:czc_Keyword) WHERE p.paperId =" + paperId +
                                        " AND k.keyword_name =\'" + keywordArray[i].trim()
                                        + "\' CREATE (p)-[r:INVOLVED]->(k)");
                            }
                        }
                    }

                    //添加author-organ关系
                    if (!TextUtils.isEmpty(authorOrgan)) {
                        String a = null;
                        String o = null;
                        String[] aoArray = authorOrgan.split(";");
                        for (int i = 0; i < aoArray.length; i++) {
                            if (aoArray[i].contains("[")) {
                                a = aoArray[i].split("\\[")[0].trim();
                                o = aoArray[i].split("\\[")[1].trim()
                                        .replace("]", "")
                                        .replace("\"", "\\\"").replace("\'", "\\\'");
                                Neo4jRest.execute("MATCH (a:czc_Author),(o:czc_Organ) WHERE a.authorId = " +
                                        authorMap.get(a) + " AND o.org_name = \'" + o
                                        + "\' CREATE (a)-[r:BELONG]->(o)");
                            }
                        }
                    }
                    //endtime = System.nanoTime();
                    //logger.info("statge5: " + (endtime-begintime)/1000 );
                } catch (Exception e) {

                    logger.warn("处理数据时出错");
                }
            }
        }


    }
}