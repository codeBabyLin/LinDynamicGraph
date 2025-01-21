package cn.Lin.EasyDynamicGraph;

import cn.Lin.EasyDynamicGraph.entities.InMemoryEntity;
import cn.Lin.EasyDynamicGraph.entities.InMemoryGraph;
import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.timeindex.timestore.disk.SnapshotGraphStore;
import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class SnapshotGraphStoreTest {


    String path = "./TimeStore";
    SnapshotGraphStore sgs;


    private void delFile(File file){
        if(file.isDirectory()){
            for(File childFile: file.listFiles()){
                delFile(childFile);
            }
        }
        else{
            file.delete();
        }
    }
    @Before
    public void init(){
        File file = new File(path);
        delFile(file);

    }

    @Test
    public void SnapshotTest(){
        sgs = new SnapshotGraphStore(path);
        InMemoryGraph graph1 = InMemoryGraph.createGraph();
        InMemoryNode node1 = new InMemoryNode(1,1);
        InMemoryNode node2 = new InMemoryNode(2,1);
        InMemoryRelationship r1 = new InMemoryRelationship(1,1,2,0,2);
        graph1.updateNode(node1);
        graph1.updateNode(node2);
        //graph1.updateRelationship(r1);

        sgs.storeSnapshot(graph1,1);

        InMemoryGraph graph2 = InMemoryGraph.createGraph();
        graph2.updateNode(node1);
        graph2.updateNode(node2);
        graph2.updateRelationship(r1);

        sgs.storeSnapshot(graph2,2);


        Pair<Long,InMemoryGraph> res1 = sgs.getSnapshot(1);

        InMemoryGraph graph3 = res1.getValue();

        int size = graph3.getRelationships().size();

        Assert.assertEquals(0,size);


        Pair<Long,InMemoryGraph> res2 = sgs.getSnapshot(2);

        InMemoryGraph graph4 = res2.getValue();

        int size1 = graph4.getRelationships().size();

        Assert.assertEquals(1,size1);


    }

}
