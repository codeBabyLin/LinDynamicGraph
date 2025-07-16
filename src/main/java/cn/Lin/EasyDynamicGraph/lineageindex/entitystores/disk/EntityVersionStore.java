package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;

public class EntityVersionStore {

    private KeyValueStore entityIdCStore;
    private KeyValueStore entityIdDStore;
    private Transformer transformer;

    public EntityVersionStore(String dir){
        this.transformer = new Transformer();
        this.entityIdCStore = new KeyValueStore(new File(dir,"IdCreate").getAbsolutePath());
        this.entityIdDStore = new KeyValueStore(new File(dir,"IdDelte").getAbsolutePath());
    }
    public EntityVersionStore(String dir,Transformer transformer){
        this.transformer = transformer;
        this.entityIdCStore = new KeyValueStore(new File(dir,"IdCreate").getAbsolutePath());
        this.entityIdDStore = new KeyValueStore(new File(dir,"IdDelte").getAbsolutePath());
    }
    public boolean existEntity(long entityId){
        return this.entityIdCStore.exist(transformer.LongToByte(entityId));
    }
    public void addEntity(long entityId,long version){

        byte[]key = transformer.LongToByte(entityId);
        byte[]cValue = transformer.LongToByte(version);
        byte[]dValue = transformer.LongToByte(Long.MAX_VALUE);
        this.entityIdCStore.add(key,cValue);
        this.entityIdDStore.add(key,dValue);

    }
    public void deleteEntity(long entityId,long version){
        byte[]key = transformer.LongToByte(entityId);
        byte[]dValue = transformer.LongToByte(version);
        this.entityIdDStore.add(key,dValue);
    }
    public void deleteEntity(byte[]key,long version){
        //byte[]key = transformer.LongToByte(node);
        byte[]dValue = transformer.LongToByte(version);
        this.entityIdDStore.add(key,dValue);
    }

    public Iterator<Long> AllEntities(){
        Iterator<byte[]> iter = this.entityIdCStore.all();
        return new Iterator<Long>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Long next() {
                byte[]data = iter.next();
                long e = transformer.ByteToLong(data);
                return e;
            }
        };
    }

    public long getEntityCreateVersion(long node){
        byte[]key = transformer.LongToByte(node);
        byte[]value = this.entityIdCStore.get(key);
        long v = transformer.ByteToLong(value);
        return v;
    }
    public long getEntityDeleteVersion(long node){
        byte[]key = transformer.LongToByte(node);
        byte[]value = this.entityIdDStore.get(key);
        long v = transformer.ByteToLong(value);
        return v;
    }

    public void setEntityCreateVersion(byte[]key, byte[]value){
        this.entityIdCStore.add(key,value);
    }
    public void setEntityDeleteVersion(byte[]key, byte[]value){
        this.entityIdDStore.add(key,value);
    }

    private boolean isOk(long v, long s, long e){
        return s <= v && (v <= e);
    }
    private boolean isOk(long vs,long ve,long ds,long de){
        return ds<=vs && (ve<=de);
    }


    public Iterator<Long> AllEntitiesByVersion(Long version){
        Iterator<byte[]> iter = this.entityIdCStore.all();
        return new Iterator<Long>() {
            long temp;
            @Override
            public boolean hasNext() {
                if(iter.hasNext()){
                    boolean isCan = false;
                    while(!isCan&& iter.hasNext()){
                        byte[]key = iter.next();
                        byte[]cValue = entityIdCStore.get(key);
                        byte[]dValue = entityIdDStore.get(key);
                        long e = transformer.ByteToLong(key);
                        long vs = transformer.ByteToLong(cValue);
                        long ve = transformer.ByteToLong(dValue);
                        if(isOk(version,vs,ve)){
                            temp = e;
                            isCan = true;
                        }
                    }

                    return isCan;

                }
                else{
                    return false;
                }
            }

            @Override
            public Long next() {
                return temp;
            }
        };
    }

    public Iterator<Long> AllEntitiesByVersion(Long vStart, Long vEnd ){
        Iterator<byte[]> iter = this.entityIdCStore.all();
        return new Iterator<Long>() {
            long temp;
           @Override
            public boolean hasNext() {
                if(iter.hasNext()){
                    boolean isCan = false;
                    while(!isCan&& iter.hasNext()){
                        byte[]key = iter.next();
                        byte[]cValue = entityIdCStore.get(key);
                        byte[]dValue = entityIdDStore.get(key);
                        long e = transformer.ByteToLong(key);
                        long vs = transformer.ByteToLong(cValue);
                        long ve = transformer.ByteToLong(dValue);
                        if(isOk(vStart,vEnd,vs,ve)){
                            temp = e;
                            isCan = true;
                        }
                    }

                    return isCan;

                }
                else{
                    return false;
                }
            }

            @Override
            public Long next() {
                return temp;
            }
        };
    }

    public void flush(){
        this.entityIdCStore.flush();
        this.entityIdDStore.flush();
    }


}
