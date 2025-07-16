package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.Property;
import cn.Lin.EasyDynamicGraph.entities.PropertyType;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import io.netty.buffer.ByteBuf;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// key   ---nId pid ts,te  value ---v
public class PropertyVersionStore {

    private KeyValueStore propertyVersionStore;

    private PropertyNameIdStore propertyNameIdStore;

    private Transformer transformer;
    public PropertyVersionStore(String path, Transformer transformer){
        this.propertyVersionStore = new KeyValueStore(path);

        this.transformer = transformer;
        this.propertyNameIdStore = new PropertyNameIdStore(Paths.get(path,"nameIdStore").toString(),transformer);
    }

    public void updateProperty(long id, String propertyName,long startTime,long endTime, Object data){
        long propertyId = this.propertyNameIdStore.getNameid(propertyName);
        updateProperty(id,propertyId,startTime,endTime,data);
    }


    public void updateProperty(long id, long propertyId,long startTime,long endTime, Object data){
        long [] tempKey = {id,propertyId,startTime,endTime};
        byte[] key = transformer.LongArrayToByteWithOutSize(tempKey);
        byte[] value = transformer.ObjectToByte(data);
        this.propertyVersionStore.add(key,value);

    }

    public List<Property> getProperties(long id){
        long tempKey[]= {id};
        byte[] prefix = transformer.LongArrayToByteWithOutSize(tempKey);
        ArrayList<Property> properties = new ArrayList<>();
        Iterator<byte[]> iter = this.propertyVersionStore.allWithPrefix(prefix);
        while(iter.hasNext()){
            byte[]key = iter.next();
            ByteBuf byteBuf = transformer.getByteBuf(key);
            long nodeId = byteBuf.readLong();
            long propertyId = byteBuf.readLong();
            String propertyName = this.propertyNameIdStore.getIdName((int) propertyId);
            long startTime = byteBuf.readLong();
            long endTime = byteBuf.readLong();

            byte[] value =this.propertyVersionStore.get(key);
            Object obj = transformer.ByteToObject(value);
            Property e = new Property(PropertyType.getType(obj),propertyName,obj);
            properties.add(e);

        }
        return properties;
    }
    public List<Property> getPropertiesByTime(long id,long timeStamp){
        long tempKey[]= {id};
        byte[] prefix = transformer.LongArrayToByteWithOutSize(tempKey);
        ArrayList<Property> properties = new ArrayList<>();
        Iterator<byte[]> iter = this.propertyVersionStore.allWithPrefix(prefix);
        while(iter.hasNext()){
            byte[]key = iter.next();
            ByteBuf byteBuf = transformer.getByteBuf(key);
            long nodeId = byteBuf.readLong();
            long propertyId = byteBuf.readLong();
            String propertyName = this.propertyNameIdStore.getIdName((int) propertyId);
            long startTime = byteBuf.readLong();
            long endTime = byteBuf.readLong();

            byte[] value =this.propertyVersionStore.get(key);
            Object obj = transformer.ByteToObject(value);
            Property e = new Property(PropertyType.getType(obj),propertyName,obj);
            if(startTime<= timeStamp && timeStamp<= endTime) {
                properties.add(e);
            }

        }
        return properties;
    }





    public Object[] getProperty(long id, String propertyName){
        long propertyId = this.propertyNameIdStore.getNameid(propertyName);
        return getProperty(id,propertyId);
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
