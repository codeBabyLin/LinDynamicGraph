package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class RelationStore {

    private KeyValueStore relationStore;
    private Transformer transformer;

    private Map<Long,long[]> cashRelationships;

    public RelationStore(String dir){

        this.relationStore = new KeyValueStore(dir);
        this.transformer = new Transformer();
        this.cashRelationships = new TreeMap<>();
    }

    public RelationStore(String dir, Transformer transformer){

        this.relationStore = new KeyValueStore(dir);
        this.transformer = transformer;
        this.cashRelationships = new TreeMap<>();
    }

    public void addRelation(InMemoryRelationship rel){
        byte[] key = transformer.LongToByte(rel.getEntityId());
        ByteBuf byteBuf = transformer.getByteBuf();

        byteBuf.writeLong(rel.getStartNode());
        byteBuf.writeLong(rel.getEndNode());
        byteBuf.writeInt(rel.getType());
        byte[] value = byteBuf.array();
        this.relationStore.add(key,value);
    }
    public long getStartNode(long relId){
        if(cashRelationships.get(relId)!=null){
            long[] res = cashRelationships.get(relId);
            return res[0];
        }
        else{
            byte[] key = transformer.LongToByte(relId);
            byte[]value = relationStore.get(key);
            ByteBuf byteBuf =transformer.getByteBuf(value);
            long startNode = byteBuf.readLong();
            long endNode = byteBuf.readLong();
            int type = byteBuf.readInt();
            cashRelationships.put(relId, new long[]{startNode, endNode, type});
            return startNode;
        }
    }

    public long getEndNode(long relId){
        if(cashRelationships.get(relId)!=null){
            long[] res = cashRelationships.get(relId);
            return res[1];
        }
        else{
            byte[] key = transformer.LongToByte(relId);
            byte[]value = relationStore.get(key);
            ByteBuf byteBuf =transformer.getByteBuf(value);
            long startNode = byteBuf.readLong();
            long endNode = byteBuf.readLong();
            int type = byteBuf.readInt();
            cashRelationships.put(relId, new long[]{startNode, endNode, type});
            return endNode;
        }
    }
    public long getType(long relId){
        if(cashRelationships.get(relId)!=null){
            long[] res = cashRelationships.get(relId);
            return res[2];
        }
        else{
            byte[] key = transformer.LongToByte(relId);
            byte[]value = relationStore.get(key);
            ByteBuf byteBuf =transformer.getByteBuf(value);
            long startNode = byteBuf.readLong();
            long endNode = byteBuf.readLong();
            int type = byteBuf.readInt();
            cashRelationships.put(relId, new long[]{startNode, endNode, type});
            return type;
        }
    }

}
