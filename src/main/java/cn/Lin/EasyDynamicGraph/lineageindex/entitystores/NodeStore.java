package cn.Lin.EasyDynamicGraph.lineageindex.entitystores;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface NodeStore {
    void addNodes(List<InMemoryNode> nodes) throws IOException;

    Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException;

    List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException;

    List<InMemoryNode> getAllNodes(long timestamp) throws IOException;

    void flushIndex();

    void reset();

    void shutdown() throws IOException;

    void setDiffThreshold(int threshold);
}