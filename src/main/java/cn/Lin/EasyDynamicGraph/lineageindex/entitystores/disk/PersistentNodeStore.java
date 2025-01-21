package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.NodeStore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class PersistentNodeStore implements NodeStore {

    PropertyVersionStore propertyVersionStore;
    LabelVersionStore labelVersionStore;
    EntityVersionStore entityVersionStore;

    @Override
    public void addNodes(List<InMemoryNode> nodes) throws IOException {

    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        return Optional.empty();
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryNode> getAllNodes(long timestamp) throws IOException {
        return null;
    }

    @Override
    public void flushIndex() {

    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void setDiffThreshold(int threshold) {

    }
}
