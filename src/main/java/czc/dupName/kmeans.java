package czc.dupName;

import com.hankcs.hanlp.mining.cluster.ClusterAnalyzer;
import database.Constant;
import database.MysqlDatabase;
import org.apache.http.util.TextUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class kmeans {
    public static void main(String[] args) throws SQLException {
        Connection kejsoCon = MysqlDatabase.getConnection(Constant.mysqlUrlKejso);
        final ArrayList<String> nameList = new ArrayList<String>();

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        ResultSet resultSet = kejsoCon.createStatement().executeQuery("SELECT name FROM scholar_samename");
        System.out.println("名字筛选完成！");
        int nameCount = 0;
        while (resultSet.next()) {
            //获取作者名称
            final String name = resultSet.getString("name");
            if (nameList.contains(name)) continue;
            nameList.add(name);
            //一个名字开一个线程
            int finalNameCount = ++nameCount;
            executorService.submit(
                    new Runnable() {
                        public void run() {
                            int dupCount = 0;
                            try {
                                ResultSet result = null;
                                List<String> list = new ArrayList<>();
                                Connection kgCon = MysqlDatabase.getConnection(Constant.mysqlUrlKG);
                                HashMap<String, List<String>> titleMap = new HashMap<String, List<String>>();  //name、organ、year
                                ClusterAnalyzer<String> analyzer = new ClusterAnalyzer<String>();
                                for (int i = 2010; i < 2020; i++) {
                                    synchronized (this) {
                                        result = kgCon.createStatement().executeQuery("SELECT * FROM ArticleInfo_" + i + " where instr(AuthorOrgan,'" + name + "[')>0");
                                        System.out.println(finalNameCount + " " + name + " 已查询ArticleInfo_" + i);
                                    }
                                    while (result.next()) {
                                        String keywords = result.getString("Keyword").trim().replace(";", ",");
                                        if (TextUtils.isEmpty(keywords)) continue;
                                        String title = result.getString("Title");
                                        if (TextUtils.isEmpty(title)) continue;
                                        String organ = result.getString("AuthorOrgan");

                                        int index = organ.indexOf(name);
                                        organ = organ.substring(organ.indexOf("[", index) + 1, organ.indexOf("]", index));
                                        list.add(organ);
                                        list.add(name);
                                        list.add(result.getString("Year"));

                                        analyzer.addDocument(title, keywords);
                                        titleMap.put(title, list);
                                        list.clear();
                                        dupCount++;
                                    }
                                }
                                System.out.println("处理完成，共" + dupCount + "个" + name);
                                if (dupCount > 1) {
                                    List<Set<String>> titleSets = analyzer.repeatedBisection(0.5);
                                    System.out.println(titleSets);
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        }

    }
}
