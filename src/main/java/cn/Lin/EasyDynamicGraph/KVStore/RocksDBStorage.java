package cn.Lin.EasyDynamicGraph.KVStore;

import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Iterator;

public class RocksDBStorage {
    private RocksDB rockDB;
    public RocksDBStorage(RocksDB rockDB){
        this.rockDB = rockDB;
    }

    public boolean exist(byte[]key){
        return rockDB.keyMayExist(key,new Holder<>());
    }
    public byte[] get(byte[]key){
        byte[] res = null;
        try {
            res = rockDB.get(key);
        }catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }
    public void put(byte[]key,byte[]value){
        try {
            rockDB.put(key,value);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void delete(byte[]key){
        try {
            rockDB.delete(key);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public Iterator<byte[]> All(){
        RocksIterator iter = rockDB.newIterator();
        iter.seekToFirst();
        return new Iterator<byte[]>() {

            @Override
            public boolean hasNext() {
                return iter.isValid();
            }

            @Override
            public byte[] next() {
                byte[]data = iter.key();
                iter.next();
                return data;
            }
        };
    }
    //range
    public Iterator<byte[]> AllWithPrefix(byte[]prefix){
        RocksIterator iter = rockDB.newIterator();
        iter.seek(prefix);
        String prefixStr = new String(prefix);
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                //return iter.isValid();
                return iter.isValid() && new String(iter.key()).startsWith(prefixStr);
            }

            @Override
            public byte[] next() {
                byte[]data = iter.key();
                iter.next();
                return data;
            }
        };
    }



    public ArrayList<byte[]> All2(){
        RocksIterator iter = rockDB.newIterator();
        iter.seekToFirst();

        ArrayList<byte[]> res = new ArrayList<>();

        while (iter.isValid()){
            byte[]data = iter.key();
            res.add(data);
            iter.next();
        }
        return res;
    }
    //range
    public ArrayList<byte[]> AllWithPrefix2(byte[]prefix){
        RocksIterator iter = rockDB.newIterator();
        iter.seek(prefix);
        String prefixStr = prefix.toString();
        ArrayList<byte[]> res = new ArrayList<>();

        while(iter.isValid() && iter.key().toString().startsWith(prefixStr)){
            byte[]data = iter.key();
            res.add(data);
            iter.next();
        }

      return res;
    }



    //from


    public void flush(){
        try {
            this.rockDB.flush(new FlushOptions());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

