package word2vec;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import database.MysqlDatabase;
import org.apache.log4j.Logger;
import processing.MysqlNeo4jRest;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Segment {
    private static Logger logger = Logger.getLogger(Segment.class);

    public static void main(String[] args) throws IOException, SQLException {

        Connection mysqlConnection = MysqlDatabase.getConnection();


        for(int i = 0;i<9 ;i++) {
        /*
            获得表格总行数，计算循环次数
         */
            ResultSet result = mysqlConnection.createStatement().executeQuery("SELECT count(*) as count FROM ArticleInfo_201"+i);
            result.next();
            int row = result.getInt("count");
            int allRounds = (int) (row / 1000);
            logger.info("获取到 " + row + " 条数据");

            //加载停用词典
            Set<String> stopWords = readTxtFileIntoStringSet("stop.txt");


            for (int round = 0; round < allRounds; round++) {
                logger.info("已完成 201"+i+" 年数据，" + (round / (float) allRounds) * 100 + "%");
                //写入分词结果的目的文件
                FileWriter fw = new FileWriter("paper_segment.txt", true);
                PrintWriter pw = new PrintWriter(fw);

                PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_201"+i+" limit ?,1000");
                mysqlPs.setInt(1, 1000 * round);
                ResultSet mResult = mysqlPs.executeQuery();
                int line = 0;

                //依次对每个语料进行分词
                while (mResult.next()) {

                    try {
                        //论文的标题
                        String title = mResult.getString("Title");
                        //论文的摘要
                        String abst = mResult.getString("Abstract");
                        List<Term> titleList = HanLP.segment(title);
                        if (!titleList.isEmpty()) {
                            for (Term term : titleList) {
                                //判断是否是停用词
                                if (stopWords.contains(term.word))
                                    continue;
                                pw.write(term.word + " ");
                            }
                            pw.write("\n");
                        }

                        List<Term> abstList = HanLP.segment(abst);
                        if (!abstList.isEmpty()) {
                            for (Term term : abstList) {
                                //判断是否是停用词
                                if (stopWords.contains(term.word))
                                    continue;
                                pw.write(term.word + " ");
                            }
                            pw.write("\n");
                        }
                    } catch (Exception e) {
                    }

                }
                pw.flush();
                fw.flush();
                fw.close();
                pw.close();

            }
        }

    }

    public static Set<String> readTxtFileIntoStringSet(String filePath)
    {
        Set<String> set = new TreeSet<String>();
        try
        {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists())
            { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;

                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    set.add(lineTxt);
                }
                bufferedReader.close();
                read.close();
            }
            else
            {
                System.out.println("找不到指定的文件");
            }
        }
        catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

        return set;
    }
}
