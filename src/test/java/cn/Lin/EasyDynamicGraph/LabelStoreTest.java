package cn.Lin.EasyDynamicGraph;

import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk.LabelVersionStore;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class LabelStoreTest {
    String path = "./LabelVersionStore";
    LabelVersionStore labelVersionStore;
    Transformer transformer;

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
    public void testTop(){
        transformer = new Transformer();
        labelVersionStore = new LabelVersionStore(path,transformer);
        labelVersionStore.addLabel(1,0,5,"111");
        labelVersionStore.addLabel(1,6,7,"122");
        labelVersionStore.addLabel(2,0,10,"2333");
        labelVersionStore.addLabel(3,0,10,"36666");
        labelVersionStore.addLabel(1,8,10,"1999");

        System.out.println("size:" + labelVersionStore.size());

       String[] lables =  labelVersionStore.getLabels(3);
       for(String str: lables){
           System.out.println(str);
       }



    }


}
