package czc.dupName;

import com.hankcs.hanlp.mining.cluster.ClusterAnalyzer;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import database.Constant;
import database.MysqlDatabase;
import org.apache.http.util.TextUtils;
import org.neo4j.driver.v1.Record;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
        Connection kgCon = MysqlDatabase.getConnection(Constant.mysqlUrlKG);
        final ArrayList<String> nameList = new ArrayList<String>();

        ExecutorService executorService = Executors.newFixedThreadPool(100);

        ResultSet resultSet = kejsoCon.createStatement().executeQuery("SELECT name FROM scholar_samename");
        System.out.println("名字筛选完成！");
        while (resultSet.next()) {
            //获取作者名称
            final String name = resultSet.getString("name");
            if (nameList.contains(name)) continue;
            nameList.add(name);
            //一个名字开一个线程
            executorService.submit(new Runnable() {
                public void run() {
                    int dupCount = 0;
                    try {
                        ResultSet result = null;
                        List<String> list = new ArrayList<>();
                        List<Set<String>> titleSets = new ArrayList<>();
                        HashMap<String, List<String>> titleMap = new HashMap<String, List<String>>();  //name、organ、year
                        ClusterAnalyzer<String> analyzer = new ClusterAnalyzer<String>();
                        for (int i = 2010; i < 2020; i++) {
                            result = kgCon.createStatement().executeQuery("SELECT * FROM ArticleInfo_" + i + " where instr(AuthorOrgan,'" + name + "[')>0");
                            while (result.next()) {
                                String keywords = result.getString("Keyword").trim().replace(";", ",");
                                if (TextUtils.isEmpty(keywords)) continue;
                                String title = result.getString("Title");
                                if (TextUtils.isEmpty(title)) continue;
                                String organ = result.getString("AuthorOrgan");

                                int index = organ.indexOf(name);
//                        if (organ.indexOf("[", index) != (index + name.length())) continue;
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
                            titleSets = analyzer.repeatedBisection(1.0);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

    }
}
