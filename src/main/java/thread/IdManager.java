package thread;

import database.Neo4jRest;
import org.apache.log4j.Logger;

public class IdManager {
    private static Logger logger = Logger.getLogger(IdManager.class);

    private static int authorId;
    private static int paperId;
    private static int orgId;
    private static int journalId;
    private static int keywordId;


    public IdManager() {
        this.authorId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Author) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        this.paperId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Paper) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        this.orgId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Organ) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        this.journalId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Journal) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
        this.keywordId = Integer.valueOf(Neo4jRest.getExecuteData("MATCH (o:czc_Keyword) RETURN COUNT(o) AS count")
                .getJSONObject(0).getJSONArray("row").get(0).toString());
    }

    public int getAuthorId(){
        synchronized (this) {
            return authorId++;
        }
    }

    public int getPaperId(){
        synchronized (this) {
            logger.info("PaperId: "+paperId);
            return paperId++;
        }
    }

    public int getOrgId(){
        synchronized (this) {
            return orgId++;
        }
    }

    public int getJournalId(){
        synchronized (this) {
            return journalId++;
        }
    }

    public int getKeywordId(){
        synchronized (this) {
            return keywordId++;
        }
    }



}
