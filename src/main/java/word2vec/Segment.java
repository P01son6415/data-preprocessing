package word2vec;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import database.MysqlDatabase;
import org.apache.log4j.Logger;
import thread.MysqlManager;
import thread.ParseData;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class Segment extends Thread {
    private static Logger logger = Logger.getLogger(Segment.class);

    private Thread t;
    private String threadName;
    private static SyncFileWriter fileWriter;

    private static DataDelivery dataDelivery;

    static ConcurrentHashMap<Integer,Integer> activeThread = new ConcurrentHashMap<Integer, Integer>();

    Segment(String name) throws IOException{
        threadName = name;
        System.out.println("Creating " +  threadName );
        this.fileWriter = new SyncFileWriter("paper_segment.txt");
    }


    @Override
    public void run() {

        Connection mysqlConnection = MysqlDatabase.getConnection();


        try {

            dataDelivery = new  DataDelivery(2010,2012,1000);


            ResultSet mResult = dataDelivery.getBouch();

            //加载停用词典
            Set<String> stopWords = readTxtFileIntoStringSet("stop.txt");




                //依次对每个语料进行分词
                while (mResult.next()) {

                    //论文的标题
                    String title = mResult.getString("Title");
                    //论文的摘要
                    String abst = mResult.getString("Abstract");
                    List<Term> titleList = HanLP.segment(title);
                    if (!titleList.isEmpty()) {
                        StringBuilder titleBuffer = new StringBuilder();
                        for (Term term : titleList) {
                            //判断是否是停用词
                            if (stopWords.contains(term.word))
                                continue;
                            titleBuffer.append(term.word + " ");
                        }
                        titleBuffer.append("\n");
                        fileWriter.writeAndFlush(titleBuffer.toString());
                    }


                    List<Term> abstList = HanLP.segment(abst);
                    if (!abstList.isEmpty()) {
                        StringBuilder abstBuffer = new StringBuilder();
                        for (Term term : abstList) {
                            //判断是否是停用词
                            if (stopWords.contains(term.word))
                                continue;
                            abstBuffer.append(term.word + " ");
                        }
                        abstBuffer.append("\n");
                        fileWriter.writeAndFlush(abstBuffer.toString());
                    }


                }





        }catch (Exception e){}

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
class TestThread {

    public static void main(String args[]) {

        while (true){
            try {
            if(Segment.activeThread.size()<12) {
                Segment T1 = new Segment("T");
                T1.start();
            }


                Thread.sleep(2000);
            }catch (Exception e){}

        }

    }
}