package processing;

import database.Neo4jDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws SQLException {
        Session session = Neo4jDatabase.getSession();

        long startTime =  System.currentTimeMillis();

        session.run("MATCH (p:czc_Paper) WHERE p.paper_name = \'123\'  RETURN p");
        session.run("MATCH (p:czc_Paper) WHERE p.paper_name = \'纳米二氧化钛和纳米氧化锌的Ames试验\'  RETURN p");

        long endTime =  System.currentTimeMillis();
        System.out.println((endTime-startTime));
    }
}
