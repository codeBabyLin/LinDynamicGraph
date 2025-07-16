package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class PropertyNameIdStore {

    private KeyValueStore propertyNameIdStore;
    private Transformer transformer;
    private HashMap<String,Integer> nameIdArray;
    private HashMap<Integer,String> idNameArray;


    public PropertyNameIdStore(String path, Transformer transformer){
        this.propertyNameIdStore = new KeyValueStore(path);
        this.transformer = transformer;
        this.idNameArray = new HashMap<>();
        this.nameIdArray = new HashMap<>();
        Iterator<byte[]> iter = this.propertyNameIdStore.all();
        while(iter.hasNext()){
            byte[] key = iter.next();
            byte[] value = this.propertyNameIdStore.get(key);
            long nameId = this.transformer.ByteToLong(key);
            String idName = this.transformer.ByteToString(value);
            this.nameIdArray.put(idName, (int) nameId);
            this.idNameArray.put((int) nameId,idName);
        }
    }

    private void addNameId(String name){
        int id = this.nameIdArray.size()+1;
        this.idNameArray.put(id,name);
        this.nameIdArray.put(name,id);
        this.propertyNameIdStore.add(transformer.IntToByte(id),transformer.StringToByte(name));

    }

    public int getNameid(String name){

        if(this.nameIdArray.get(name)==null){
            addNameId(name);
        }

        return this.nameIdArray.get(name);
    }
    public String getIdName(int id){
        return this.idNameArray.get(id);
    }




    public int size(){
        return this.idNameArray.size();
    }


}
