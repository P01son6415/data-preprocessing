package czc;

import database.Constant;
import database.MysqlDatabase;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class organChange {
    public static void main(String[] args) throws SQLException, IOException {
        Connection kgCon = MysqlDatabase.getConnection(Constant.mysqlUrlKG);
        Connection kejsoCon = MysqlDatabase.getConnection(Constant.mysqlUrlKejso);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        FileWriter locfw = new FileWriter("organLocat.txt", true);
        PrintWriter locpw = new PrintWriter(locfw);
        FileWriter artfw = new FileWriter("organArticle.txt", true);
        PrintWriter artpw = new PrintWriter(artfw);

        ResultSet organResult = kejsoCon.createStatement().executeQuery("select name,lat,lng from organization_area where lng is not null and lat is not null");
        while (organResult.next()) {
            String orgName = organResult.getString("name");
            try {
                String lng = String.format("%.14f", organResult.getDouble("lng"));
                String lat = String.format("%.14f", organResult.getDouble("lat"));
                String organLocat = "\"" + orgName + "\"" + ": [" + lng + "," + lat + "],\n";
                locpw.write(organLocat);
                locpw.flush();

                StringBuffer organArticles = new StringBuffer();
                organArticles.append("[\"" + orgName + "\"");
                for (int i = 2010; i < 2020; i++) {
                    ResultSet articleResult = kgCon.createStatement().executeQuery("select count(*) from ArticleInfo_" + i
                            + " WHERE instr(Organ,\"" + orgName + "\")>0 and Title is not null");
                    articleResult.next();
                    int count = articleResult.getInt(1);
                    organArticles.append("," + count);
                    if (i == 2019) organArticles.append(" ],\n");
                    System.out.println(orgName + " ArticleInfo_" + i + "已完成");
                }
                artpw.write(organArticles.toString());
                artpw.flush();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        locpw.close();
        artpw.close();
    }
}
