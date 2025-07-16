package cn.Lin.EasyDynamicGraph.lineageindex;

import cn.Lin.EasyDynamicGraph.entities.*;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.NodeStore;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.RelationshipStore;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory.InMemoryNodeStore;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory.LinkedListRelationshipStore;
import cn.Lin.EasyDynamicGraph.utils.IntCircularList;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class InMemoryLineageStore implements LineageStore {
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;

    // Thread local variables for graph retrieval
    private static final ThreadLocal<IntCircularList> bfsQueue = ThreadLocal.withInitial(IntCircularList::new);
    private final ThreadLocal<RoaringBitmap> visitedNodes = ThreadLocal.withInitial(RoaringBitmap::new);

    public InMemoryLineageStore() {
        nodeStore = new InMemoryNodeStore();
        relationshipStore = new LinkedListRelationshipStore();
    }

    public void addNodes(List<InMemoryNode> nodes) throws IOException {
        nodeStore.addNodes(nodes);
    }

    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {
        relationshipStore.addRelationships(rels);
    }

    public List<InMemoryNode> getAllNodes(long timestamp) throws IOException {
        return nodeStore.getAllNodes(timestamp);
    }

    public List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException {
        return relationshipStore.getAllRelationships(timestamp);
    }

    @Override
    public void flushIndexes() {}

    public void reset() {
        nodeStore.reset();
        relationshipStore.reset();
    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        return nodeStore.getNode(nodeId, timestamp);
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        return nodeStore.getNode(nodeId, startTime, endTime);
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        return relationshipStore.getRelationship(relId, timestamp);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException {
        return relationshipStore.getRelationship(relId, startTime, endTime);
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException {
        return relationshipStore.getRelationships(nodeId, direction, timestamp);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        return relationshipStore.getRelationships(nodeId, direction, startTime, endTime);
    }

    @Override
    public List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp)
            throws IOException {
        if (direction != RelationshipDirection.OUTGOING) {
            throw new UnsupportedOperationException("Supporting only outgoing edges now");
        }

        List<InMemoryNode> result = new ArrayList<InMemoryNode>();
        RoaringBitmap bitmap = visitedNodes.get();
        IntCircularList queue = bfsQueue.get();
        queue.clear();

        queue.add((int) nodeId);
        for (int i = 0; i < hops; i++) {

            bitmap.clear();
            int queueSize = queue.size();
            for (int j = 0; j < queueSize; ++j) {
                int currentNode = queue.poll();
                List<InMemoryRelationship> rels = getRelationships(currentNode, direction, timestamp);
                for (InMemoryRelationship r : rels) {
                    // Outgoing edge
                    if (r.getStartNode() == currentNode) {

                        // Get target node
                        int targetId = (int) r.getEndNode();
                        Optional<InMemoryNode> targetNode = nodeStore.getNode(targetId, timestamp);
                        if (targetNode.isPresent() && !bitmap.contains(targetId)) {
                            targetNode.get().setHop(i);
                            result.add(targetNode.get());
                            queue.add(targetId);
                            bitmap.add(targetId);
                        }
                    }
                }
            }
        }

        return result;
    }

    @Override
    public List<List<InMemoryNode>> expand(
            long nodeId, RelationshipDirection direction, int hops, long startTime, long endTime, long timeStep)
            throws IOException {
        List<List<InMemoryNode>> result = new ArrayList<List<InMemoryNode>>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(expand(nodeId, direction, hops, time));
        }
        return result;
    }

    @Override
    public InMemoryGraph getWindow(long startTime, long endTime) throws IOException {
        // Requires allnodes and allrels scan
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException {
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public InMemoryGraph getGraph(long timestamp) throws IOException {
        List<InMemoryNode> nodes = getAllNodes(timestamp);
        List<InMemoryRelationship> rels = getAllRelationships(timestamp);

        InMemoryGraph graph = InMemoryGraph.createGraph();
        for (InMemoryNode n : nodes) {
            graph.updateNode(n);
        }
        for (InMemoryRelationship r : rels) {
            graph.updateRelationship(r);
        }
        return graph;
    }

    @Override
    public List<InMemoryGraph> getGraph(long startTime, long endTime, long timeStep) throws IOException {
        List<InMemoryGraph> result = new ArrayList<InMemoryGraph>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(getGraph(time));
        }
        return result;
    }

    @Override
    public List<InMemoryEntity> getDiff(long startTime, long endTime) {
        // Requires allnodes and allrels scan
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public void shutdown() {}
}

