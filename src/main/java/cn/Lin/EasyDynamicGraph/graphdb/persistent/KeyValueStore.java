package cn.Lin.EasyDynamicGraph.graphdb.persistent;

import cn.Lin.EasyDynamicGraph.KVStore.RocksDBFactory;
import cn.Lin.EasyDynamicGraph.KVStore.RocksDBStorage;

import java.util.ArrayList;
import java.util.Iterator;

public class KeyValueStore {

    private RocksDBStorage db;
    public KeyValueStore(String dataPath){
        this.db = RocksDBFactory.getDB(dataPath);
    }
    public void add(byte[] key, byte[] value){
        this.db.put(key,value);
    }
    public void remove(byte[] key){
        this.db.delete(key);
    }
    public byte[] get(byte[] key){
        return this.db.get(key);
    }

    public Iterator<byte[]> all(){
        return this.db.All();
    }

    public ArrayList<byte[]> all2(){
        return this.db.All2();
    }


    public Iterator<byte[]> allWithPrefix(byte[] prefix) {
        return this.db.AllWithPrefix(prefix);
    }
    public ArrayList<byte[]> allWithPrefix2(byte[] prefix) {
        return this.db.AllWithPrefix2(prefix);
    }



    public boolean exist(byte[]key){
        return this.db.exist(key);
    }
    public void flush(){
        this.db.flush();
    }
}
