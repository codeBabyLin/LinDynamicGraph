package org.neo4j;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class BaseTest {
    public String path = "F:\\EasyDynamicGraphStore";
    public GraphDatabaseService graphDb;

    public void delFile(File file){
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for (File value : files) {
                delFile(value);
            }
        }
        file.delete();
    }

    public void registerShutdownHook(GraphDatabaseService graphDb){
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        }));
    }
    @Before
    public void init(){
        delFile(new File(path));
    }
    @After
    public void close(){
        if(graphDb !=null){
            graphDb.shutdown();
        }
    }

}