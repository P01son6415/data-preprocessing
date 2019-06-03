package word2vec;

import database.Constant;
import database.MysqlDatabase;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataDelivery {
    private static Logger logger = Logger.getLogger(DataDelivery.class);

    static int[] round;
    static int[] finished ={0,0,0,0,0,0,0,0,0,0,0};
    int bunchSize;  //每个ResultSet包含多少数据
    int from;   //起始年份
    int to;     //结束年份
    Connection mysqlConnection = MysqlDatabase.getConnection(Constant.mysqlUrlKG);

    public DataDelivery(int from,int to, int bunchSize) throws SQLException {
        this.from = from;
        this.to = to;
        this.bunchSize = bunchSize;
        this.round = new int[to-from+1];
        for(int i = 0,j = from;j<=to;j++,i++){

            ResultSet result = mysqlConnection.createStatement().executeQuery("SELECT count(*) as count FROM ArticleInfo_201"+i);
            result.next();
            int row = result.getInt("count");
            round[i] = (int) (row / bunchSize);

            logger.info("从 "+j+" 年获取到 " + row + " 条数据");
        }
    }

    public ResultSet getBatch() throws SQLException{
        synchronized (this){
            ResultSet mResult = null;
            //迭代所有年份
            for(int i=0,j = from;j<to;j++,i++){
                //迭代round次
                while(finished[i]<round[i]) {
                    PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_" + j + " limit ?," + bunchSize);
                    mysqlPs.setInt(1, bunchSize * finished[i]);
                    finished[i]++;
                    mResult = mysqlPs.executeQuery();
                    logger.info("第 "+j+" 年第 "+finished[i]+" 轮");
                    return mResult;

                }
            }
            return mResult;

        }
    }

}
