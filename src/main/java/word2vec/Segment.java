package word2vec;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import database.MysqlDatabase;
import org.apache.log4j.Logger;
import processing.MysqlNeo4jRest;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Segment {
    private static Logger logger = Logger.getLogger(Segment.class);

    public static void main(String[] args) throws IOException, SQLException {

        Connection mysqlConnection = MysqlDatabase.getConnection();

        //写入分词结果的目的文件
        FileWriter fw = new FileWriter("segment_2010.txt",true);
        PrintWriter pw = new PrintWriter(fw);
        /*
            获得表格总行数，计算循环次数
         */
        ResultSet result = mysqlConnection.createStatement().executeQuery("SELECT count(*) as count FROM ArticleInfo_2010");
        result.next();
        int row = result.getInt("count");
        int allRounds = (int) (row / 1000);
        logger.info("获取到 " + row + " 条数据");



        for (int round = 0; round < allRounds; round++) {
            PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_2010 limit ?,1000");
            mysqlPs.setInt(1, 1000 * round);
            ResultSet mResult = mysqlPs.executeQuery();
            int line = 0;
            //依次从每行数据中提取实体、建立关系
            while (mResult.next()) {
                line++;
                if (line % 100 == 0) {
                    logger.info("当前已完成第 " + (round + 1) + " 轮，" + (line / 1000.0) * 100 + "%");
                }

                String title =  mResult.getString("Title");
                String abst = mResult.getString("Abstract");

                List<Term> titleList = HanLP.segment(title);
                for (Term term:titleList) {
                    pw.write(term.word+" ");
                }
                pw.write("\n");

                List<Term> abstList = HanLP.segment(abst);
                for (Term term:abstList) {
                    pw.write(term.word+" ");
                }
                pw.write("\n");

            }


        }


    }
}
