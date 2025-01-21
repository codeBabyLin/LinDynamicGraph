package cn.Lin.EasyDynamicGraph.lineageindex;

import cn.Lin.EasyDynamicGraph.HistoryStore;
import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;

import java.io.IOException;
import java.util.List;

public interface LineageStore extends HistoryStore {
    void addNodes(List<InMemoryNode> nodes) throws IOException;

    void addRelationships(List<InMemoryRelationship> rels) throws IOException;

    List<InMemoryNode> getAllNodes(long timestamp) throws IOException;

    List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException;

    void flushIndexes();

    void reset();
}