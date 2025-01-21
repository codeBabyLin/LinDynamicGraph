package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.NodeStore;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class InMemoryNodeStore implements NodeStore {
    // (K - V) => K: nodeId, V: node mutations ordered by timestamp
    private final EnhancedTreeMap<Long, InMemoryNode> nodeLineage;

    public InMemoryNodeStore() {
        nodeLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
    }

    public void addNodes(List<InMemoryNode> nodes) {
        for (InMemoryNode n : nodes) {
            InMemoryNode nodeClone = n.copy();
            nodeLineage.put(nodeClone.getEntityId(), nodeClone);
        }
    }

    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) {
        return nodeLineage.get(nodeId, timestamp);
    }

    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) {
        return nodeLineage.rangeScanByTime(nodeId, startTime, endTime);
    }

    public List<InMemoryNode> getAllNodes(long timestamp) {
        return nodeLineage.getAll(timestamp);
    }

    @Override
    public void flushIndex() {}

    public void reset() {
        nodeLineage.reset();
    }

    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}