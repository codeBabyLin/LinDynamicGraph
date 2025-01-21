package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;


import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

// key  label+time   value string
public class LabelVersionStore {

    private KeyValueStore labelVersionStore;
    private Transformer transformer;
    public LabelVersionStore(String path, Transformer transformer){
        this.labelVersionStore = new KeyValueStore(path);
        this.transformer = transformer;
    }




    public void addLabel(long id, long starTime, long endTime, String label){
        long tempKey[]= {id,starTime,endTime};
        byte[] key = transformer.LongArrayToByteWithOutSize(tempKey);
        byte[]value = transformer.StringToByte(label);
        this.labelVersionStore.add(key,value);

    }
    public String[] getLabels(long id){

        byte[] prefix = transformer.LongToByte(id);

        ArrayList<String>  lableName = new ArrayList<>();

        Iterator<byte[]> iter = this.labelVersionStore.allWithPrefix(prefix);
        while(iter.hasNext()){
            byte[]key = iter.next();
            byte[] value =this.labelVersionStore.get(key);
            String str = transformer.ByteToString(value);
            lableName.add(str);

        }

        String [] res = new String[lableName.size()];
        lableName.toArray(res);
        return res;
    }
    public int size(){
        return this.labelVersionStore.all2().size();
    }

}
