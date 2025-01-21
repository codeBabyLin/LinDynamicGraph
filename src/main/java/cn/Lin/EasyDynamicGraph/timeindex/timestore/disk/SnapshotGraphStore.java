package cn.Lin.EasyDynamicGraph.timeindex.timestore.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryGraph;
import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.KeyValueStore;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import io.netty.buffer.ByteBuf;
import javafx.util.Pair;

public class SnapshotGraphStore {

    private KeyValueStore snapshotGraphStore;
    private Transformer transformer;
    public SnapshotGraphStore(String dataPath){
        this.snapshotGraphStore = new KeyValueStore(dataPath);
        this.transformer = new Transformer();
    }

    public synchronized void storeSnapshot(InMemoryGraph graph, long timestamp){
        serializeGraphToDisk(graph,timestamp);
    }
    public Pair<Long, InMemoryGraph> getSnapshot(long timestamp){
        InMemoryGraph graph = getGraphFromDisk(timestamp);

        return new Pair<>(timestamp,graph);
    }
    public void flushIndex(){

    }
    public void clear(){

    }
    public void close(){

    }

    private void serializeGraphToDisk(InMemoryGraph graph, long timestamp){

        byte[]key = transformer.LongToByte(timestamp);

        ByteBuf byteBuf = transformer.getByteBuf();
        int nodeSize  = graph.getNodes().size();
        int relSize = graph.getRelationships().size();

        byteBuf.writeInt(nodeSize);
        for(InMemoryNode node: graph.getNodes()){
            byteBuf.writeLong(node.getEntityId());
        }

        byteBuf.writeInt(relSize);
        for(InMemoryRelationship relationship: graph.getRelationships()){
            byteBuf.writeLong(relationship.getEntityId());
            byteBuf.writeLong(relationship.getStartNode());
            byteBuf.writeLong(relationship.getEndNode());
            byteBuf.writeInt(relationship.getType());
        }

        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);

        this.snapshotGraphStore.add(key,data);


    }

    private InMemoryGraph getGraphFromDisk(long timestamp){
        byte[]key = transformer.LongToByte(timestamp);

        byte[] value = this.snapshotGraphStore.get(key);
        ByteBuf byteBuf = transformer.getByteBuf(value);

        InMemoryGraph graph = InMemoryGraph.createGraph();

        int nodeSize = byteBuf.readInt();
        for(int i = 0;i<nodeSize;i++){
            long entityId = byteBuf.readLong();
            InMemoryNode node = new InMemoryNode(entityId, timestamp);
            graph.updateNode(node);
        }

        int relSize = byteBuf.readInt();
        for(int i = 0;i<relSize;i++){
            long entityId = byteBuf.readLong();
            long startNode = byteBuf.readLong();
            long endNode = byteBuf.readLong();
            int type = byteBuf.readInt();
            InMemoryRelationship relationship = new InMemoryRelationship(entityId, startNode,endNode,type,timestamp);
            graph.updateRelationship(relationship);
        }



        return graph;
    }

    private void updateCache(InMemoryGraph graph, long timestamp){

    }


}
