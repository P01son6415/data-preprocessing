package database;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Neo4jRest {
    private static Logger logger = Logger.getLogger(Neo4jRest.class);


    /*
        MATCH 语句是否存在结果
     */
    public static boolean queryExist(String cypher){
        JSONObject jsonObject = execute(cypher);
        if (jsonObject.getJSONArray("results").getJSONObject(0).getJSONArray("data").size()>0){
            return true;
        }else return false;
    }

    /*
        执行语句
     */
    public static JSONObject execute(String cypher){
        //logger.info(cypher);
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials( new AuthScope("192.168.229.204", 7474),
                new UsernamePasswordCredentials(Constant.neo4juser, Constant.neo4jpassword));
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();

        //参数准备以及封装请求
        StringEntity s = new StringEntity("{\n" +
                "  \"statements\" : [ {\n" +
                "    \"statement\" : \""+cypher+"\"\n" +
                "  } ]\n" +
                "}","UTF-8");
        s.setContentEncoding("UTF-8");
        s.setContentType("application/json");//发送json数据需要设置contentType


        String url = "http://192.168.229.204:7474/db/data/transaction/commit";


        HttpPost httppost = new HttpPost(url);
        httppost.setEntity(s);
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("charset", "UTF-8");
        CloseableHttpResponse response = null;

        //获取结果
        String respString = "";
        try {
            response = httpClient.execute(httppost);
            respString = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        JSONObject jsonObject = JSONObject.parseObject(respString);
        //logger.info(jsonObject.toString());
        checkErrors(jsonObject);
        return jsonObject;

    }
    /*
        获取返回数据中的 data
     */
    public static JSONArray getExecuteData(String cypher){
        JSONObject jsonObject = execute(cypher);
        return jsonObject.getJSONArray("results").getJSONObject(0).getJSONArray("data");
    }

    private static void checkErrors(JSONObject returning){
        String s = returning.getString("errors");
        JSONArray jsonArray = JSONArray.parseArray(s);
        if(jsonArray.size()>0){
            logger.error(jsonArray.toString());
        }
    }


    public static void main(String[] args){
        JSONArray jsonObject = getExecuteData("MATCH (o:czc_Author) RETURN COUNT(o) AS count");
        System.out.println(jsonObject);
    }
}
