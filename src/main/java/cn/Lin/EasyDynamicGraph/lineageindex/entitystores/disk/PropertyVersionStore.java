package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;

import java.util.ArrayList;
import java.util.Iterator;

// key   ---nId pid ts,te  value ---v
public class PropertyVersionStore {

    private KeyValueStore propertyVersionStore;
    private Transformer transformer;
    public PropertyVersionStore(String path, Transformer transformer){
        this.propertyVersionStore = new KeyValueStore(path);
        this.transformer = transformer;
    }

    public void updateProperty(long id, long propertyId,long startTime,long endTime, Object data){
        long [] tempKey = {id,propertyId,startTime,endTime};
        byte[] key = transformer.LongArrayToByteWithOutSize(tempKey);
        byte[] value = transformer.ObjectToByte(data);
        this.propertyVersionStore.add(key,value);

    }
    public Object[] getProperty(long id, long propertyId){

        long tempKey[]= {id,propertyId};
        byte[] prefix = transformer.LongArrayToByteWithOutSize(tempKey);

        ArrayList<Object> properties = new ArrayList<>();



        Iterator<byte[]> iter = this.propertyVersionStore.allWithPrefix(prefix);
        while(iter.hasNext()){
            byte[]key = iter.next();
            byte[] value =this.propertyVersionStore.get(key);
            Object obj = transformer.ByteToObject(value);
            properties.add(obj);

        }

        Object [] res = new String[properties.size()];
        properties.toArray(res);
        return res;

    }
}
