package thread;

import database.MysqlDatabase;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlManager {
    private static Logger logger = Logger.getLogger(MysqlManager.class);
    private static int round = 0;
    private static int allRounds;

    private Connection mysqlConnection;

    public MysqlManager() {
        try {
            this.mysqlConnection = MysqlDatabase.getConnection();
            ResultSet result = mysqlConnection.createStatement().executeQuery("SELECT count(*) as count FROM ArticleInfo_2010");
            result.next();
            int row = result.getInt("count");
            this.allRounds = (int) (row / 1000);
            logger.info("获取到 " + row + " 条数据");
            logger.info("本次共处理 "+ allRounds + " 轮");
        }catch (Exception e){

        }

    }

    public ResultSet getBatch() throws SQLException{
        synchronized (this){
            logger.info("Round: " + round);
            PreparedStatement mysqlPs = mysqlConnection.prepareStatement("SELECT * FROM ArticleInfo_2010 limit ?,1000");
            mysqlPs.setInt(1, 1000 * round ++);
            ResultSet mResult = mysqlPs.executeQuery();
            return mResult;
        }
    }

    public int getRound(){
        synchronized (this) {
            return round;
        }
    }

    public int getAllRounds(){
        synchronized (this) {
            return allRounds;
        }
    }
}
