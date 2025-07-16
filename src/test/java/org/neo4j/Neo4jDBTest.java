package org.neo4j;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

public class Neo4jDBTest extends BaseTest{

    @Test
    public void testSubGraph(){
        File dataBaseDir = new File(path,"data");
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dataBaseDir);
        registerShutdownHook(graphDb);

        Transaction tx = graphDb.beginTx();

        Node node = graphDb.createNode();
        node.setProperty("name","haha");

        //long v2 = bgs.beginNextVersion();

        Node node2 = graphDb.createNode();
        node2.setProperty("age",1);

        tx.success();
        long size = graphDb.getAllNodes().stream().count();
        graphDb.shutdown();
        System.out.println(size);
    }



}
