package org.neo4j;

import org.junit.Test;
import org.neo4j.server.CommunityEntryPoint;

import java.io.File;

public class Neo4jTest {


    public static void delfile(File file){
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for(File fl: files){
                delfile(fl);
            }

        }
        file.delete();
    }

    public static void main(String[]args) {
        String NEO4J_HOME = "F:\\LinDynamicGraphStore\\Server";
        //F:\IdCode\EasyDynamicGraph\neo4j.conf
        String NEO4J_CONF = "F:\\IdCode\\LinDynamicGraph\\";
        //s"--home-dir=${NEO4J_HOME}", s"--config-dir=${NEO4J_CONF}")
        //delfile(new File(NEO4J_HOME));
        String [] ags = new String[]{"--home-dir="+NEO4J_HOME,"--config-dir="+NEO4J_CONF};
        CommunityEntryPoint.main(ags);

    }

}
