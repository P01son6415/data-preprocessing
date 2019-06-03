
package sentiment;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import database.Constant;
import database.MysqlDatabase;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


public class SegmentTest
{
    public static void main(String[] args) throws IOException, SQLException
    {
        Connection mysqlConnection = MysqlDatabase.getConnection(Constant.mysqlUrlKG);
        PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_2010 limit 100000,5");
        ResultSet mResult = mysqlPs.executeQuery();
        Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
        Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
        CRFLexicalAnalyzer crfLexicalAnalyzer = new CRFLexicalAnalyzer();
        //依次对每个语料进行分词
        while (mResult.next()) {


            //论文的标题
            String title = mResult.getString("Title");

            //论文的摘要
            //String abst = mResult.getString("Abstract");

            System.out.println("原文："+title);
            System.out.println("0. 标准分词："+ HanLP.segment(title));
            System.out.println("1. N-最短分词：" + nShortSegment.seg(title) + "\n2. 最短路分词：" + shortestSegment.seg(title));
            System.out.println("3. CRF分词：" + crfLexicalAnalyzer.analyze(title));

            List<Term> termList = IndexTokenizer.segment(title);
            System.out.print("4. 索引分词：");
            for (Term term : termList)
            {
                System.out.print(term + " ");
            }
            System.out.println("\n5. NLP分词："+NLPTokenizer.segment(title));
            System.out.println("===================================================");
        }

    }
}