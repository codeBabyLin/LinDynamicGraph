package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNeighbourhood;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationshipV2;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.RelationshipStore;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.*;

public class LinkedListRelationshipStore implements RelationshipStore {
    // (K - V) => K: edgeId, V: edge mutations ordered by timestamp
    // The edges create a "temporal" linked list of neighbourhoods for source/target nodes.
    private final EnhancedTreeMap<Long, InMemoryRelationship> relLineage;
    // (K - V) => K: nodeId, V: node's edge list mutations ordered by timestamp
    // Given a nodeId, this tree will return a pointer to the "temporal" linked list from above.
    private final EnhancedTreeMap<Long, InMemoryNeighbourhood> neighbourhoodLineage;

    private static final List<InMemoryRelationship> emptyRelList = new ArrayList<>();//List.of();

    private static final ThreadLocal<Roaring64Bitmap> visitedNodes = ThreadLocal.withInitial(Roaring64Bitmap::new);

    public LinkedListRelationshipStore() {
        relLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
        neighbourhoodLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) {
        InMemoryRelationshipV2 relClone = InMemoryRelationshipV2.createCopy(rel);
        if (relClone.isDeleted()) {
            handleDeletion(relClone);
        } else {
            addSingleRelationship(relClone);
        }
    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) {
        for (InMemoryRelationship r : rels) {
            addRelationship(r);
        }
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) {
        return relLineage.get(relId, timestamp);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) {
        return relLineage.rangeScanByTime(relId, startTime, endTime);
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp) {
        Optional<InMemoryNeighbourhood> neighbourhood = neighbourhoodLineage.get(nodeId, timestamp);
        if (!neighbourhood.isPresent() || neighbourhood.get().isDeleted()) {
            return emptyRelList;
        }

        return getRelationshipsFromNeighbourhood(neighbourhood.get(), nodeId, direction, timestamp);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) {
        List<InMemoryNeighbourhood> neighbourhoods = neighbourhoodLineage.rangeScanByTime(nodeId, startTime, endTime);

        ArrayList<List<InMemoryRelationship>> result = new ArrayList<List<InMemoryRelationship>>();
        for (InMemoryNeighbourhood n : neighbourhoods) {
            result.add(getRelationshipsFromNeighbourhood(n, nodeId, direction, n.getStartTimestamp()));
        }
        return result;
    }

    private List<InMemoryRelationship> getRelationshipsFromNeighbourhood(
            InMemoryNeighbourhood neighbourhood, long nodeId, RelationshipDirection direction, long timestamp) {
        ArrayList<InMemoryRelationship> rels = new ArrayList<InMemoryRelationship>();
        long nextRelId = neighbourhood.getEntityId();
        while (nextRelId != -1) {
            Optional<InMemoryRelationship> nextRel = relLineage.get(nextRelId, timestamp);

            nextRelId = -1;
            if (nextRel.isPresent() && nextRel.get().getStartTimestamp() <= timestamp) {
                InMemoryRelationship actualRel = nextRel.get();

                if (checkDirection(nodeId, actualRel, direction)) {
                    rels.add(actualRel);
                }

                // Get next relationship pointer from the linked list
                if (actualRel.getStartNode() == nodeId) {
                    nextRelId = actualRel.getFirstPrevRelId();
                } else {
                    nextRelId = actualRel.getSecondPrevRelId();
                }
            }
        }
        return rels;
    }

    private boolean checkDirection(long nodeId, InMemoryRelationship rel, RelationshipDirection direction) {
        return direction == RelationshipDirection.BOTH
                || (direction == RelationshipDirection.INCOMING && rel.getEndNode() == nodeId)
                || (direction == RelationshipDirection.OUTGOING && rel.getStartNode() == nodeId);
    }

    @Override
    public List<InMemoryRelationship> getAllRelationships(long timestamp) {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        relLineage.reset();
        neighbourhoodLineage.reset();
    }

    @Override
    public int numberOfRelationships() {
        int result = 0;
        for (Map.Entry<Long,List<Pair<Long,InMemoryRelationship>>> e : relLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public int numberOfNeighbourhoodRecords() {
        int result = 0;
        for (Map.Entry<Long,List<Pair<Long,InMemoryNeighbourhood>>> e : neighbourhoodLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public void flushIndexes() {}

    private void addSingleRelationship(InMemoryRelationshipV2 r) {
        // First, get the previous edge lists from src and trg nodes and update their pointers
        updateEdgePointers(r); // todo: do I need to create new edge copies here? Or do the timestamps protect accesses?

        // Then, add the relationship to the edge lineage
        insertOrMergeRelationship(r);

        // Finally, if this is a new insertion, update the node neighbourhoods.
        if (!r.isDiff()) {
            updateNeighbourhoods(r);
        }
    }

    private void updateNeighbourhoods(InMemoryRelationship r) {
        insertOrUpdateNeighbourhood(r.getStartNode(), r.getEntityId(), r.getStartTimestamp(), r.isDeleted());
        if (r.getStartNode() != r.getEndNode()) {
            insertOrUpdateNeighbourhood(r.getEndNode(), r.getEntityId(), r.getStartTimestamp(), r.isDeleted());
        }
    }

    private void insertOrUpdateNeighbourhood(long nodeId, long relId, long timestamp, boolean deleted) {
        Optional<InMemoryNeighbourhood> prevEntry = neighbourhoodLineage.getLastEntry(nodeId);
        if (prevEntry.isPresent() && prevEntry.get().getStartTimestamp() == timestamp && !deleted) {
            prevEntry.get().setEntityId(relId);
            prevEntry.get().setDeleted(false);
        } else {
            neighbourhoodLineage.put(nodeId, new InMemoryNeighbourhood(relId, timestamp, deleted));
        }
    }

    private void handleDeletion(InMemoryRelationshipV2 r) {
        // Get previous relationship and update
        Optional<InMemoryRelationship> prevRel = relLineage.getLastEntry(r.getEntityId());
        if (!prevRel.isPresent()) {
            throw new IllegalStateException("Cannot delete a non existing edge");
        }
        // Add the new value
        updateOrDeleteRelationship(r);
        updateNeighbourhoods(r);

        // Update all the pointers in the linked lists
        InMemoryRelationship actualRel = prevRel.get();
        long timestamp = r.getStartTimestamp();
        // Copy and reconnect the linked lists by removing the node in the middle.
        // In addition, update the neighbourhoods, starting from the previous pointers
        // to get a valid neighbourhood.

        // Keep track of updates relationships to re-insert them only once.
        Roaring64Bitmap visited = visitedNodes.get();
        visited.clear();

        if (actualRel.getFirstPrevRelId() != -1) {
            long nodeId = actualRel.getFirstPrevRelId();
            InMemoryRelationship node = (InMemoryRelationshipV2) relLineage.getLastEntry(nodeId).get();
            if (!node.isDeleted()) {
                InMemoryRelationship newNode = node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                newNode.updateRelId(r.getEntityId(), actualRel.getFirstNextRelId());
                insertOrMergeRelationship((InMemoryRelationshipV2) newNode);
                updateNeighbourhoods(newNode);
            }
            visited.add(nodeId);
        }

        if (actualRel.getSecondPrevRelId() != -1) {
            long nodeId = actualRel.getSecondPrevRelId();
            if (!visited.contains(nodeId)) {
                InMemoryRelationship node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    InMemoryRelationship newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getSecondNextRelId());
                    insertOrMergeRelationship((InMemoryRelationshipV2) newNode);
                    updateNeighbourhoods(newNode);
                }
                visited.add(nodeId);
            }
        }

        if (actualRel.getFirstNextRelId() != -1) {
            long nodeId = actualRel.getFirstNextRelId();
            if (!visited.contains(nodeId)) {
                InMemoryRelationship node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    InMemoryRelationship newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getFirstPrevRelId());
                    insertOrMergeRelationship((InMemoryRelationshipV2) newNode);
                    updateNeighbourhoods(newNode);
                }
                visited.add(nodeId);
            }
        }

        if (actualRel.getSecondNextRelId() != -1) {
            long nodeId = actualRel.getSecondNextRelId();
            if (!visited.contains(nodeId)) {
                InMemoryRelationship node = relLineage.getLastEntry(nodeId).get();
                if (!node.isDeleted()) {
                    InMemoryRelationship newNode =
                            (InMemoryRelationshipV2) node.copyWithoutProperties(timestamp, node.isDeleted(), true);
                    newNode.updateRelId(r.getEntityId(), actualRel.getSecondPrevRelId());
                    insertOrMergeRelationship((InMemoryRelationshipV2) newNode);
                    updateNeighbourhoods(newNode);
                }
            }
        }
    }

    private void updateOrDeleteRelationship(InMemoryRelationship r) {
        if (!relLineage.setDeleted(r.getEntityId(), r.getStartTimestamp())) {
            relLineage.put(r.getEntityId(), r);
        }
    }

    private void insertOrMergeRelationship(InMemoryRelationshipV2 r) {
        Optional<InMemoryRelationship> prevRel = relLineage.get(r.getEntityId(), r.getStartTimestamp());
        if (!prevRel.isPresent() || prevRel.get().getStartTimestamp() != r.getStartTimestamp()) {
            relLineage.put(r.getEntityId(), r);
        } else {
            prevRel.get().merge(r);
        }
    }

    /**
     *
     * Assuming that the edges of a node create a logical linked list, when updating an existing edge,
     * we perform CoW.
     *
     * @param r is the new relationship added to relLineage
     */
    private void updateEdgePointers(InMemoryRelationshipV2 r) {
        // If the edge already exits, copy its pointers
        Optional<InMemoryRelationship> prevRel = relLineage.getLastEntry(r.getEntityId());
        if (prevRel.isPresent()) {
            r.copyPointers((InMemoryRelationshipV2) prevRel.get());
        } else {
            updateEdgePointersForInsertion(r);
        }
    }

    private void updateEdgePointersForInsertion(InMemoryRelationshipV2 r) {
        // Get the previous edge that belongs to the source node
        Optional<InMemoryNeighbourhood> prevSourceEdge = neighbourhoodLineage.get(r.getStartNode(), r.getStartTimestamp());
        if (prevSourceEdge.isPresent()) {
            // Update it next pointer to the new edge if it's not the same as r
            long prevEdgeId = prevSourceEdge.get().getEntityId();
            if (prevEdgeId != r.getEntityId()) {
                InMemoryRelationship prevEdge = relLineage.get(prevEdgeId, r.getStartTimestamp()).get();
                // Check if we just added this relationship with this timestamp
                boolean addEdge = prevEdge.getStartTimestamp() != r.getStartTimestamp();
                InMemoryRelationship prevEdgeCopy = (addEdge) ? prevEdge.copy() : prevEdge;
                if (prevEdgeCopy.getStartNode() == r.getStartNode()) {
                    prevEdgeCopy.setFirstNextRelId(r.getEntityId());
                } else if (prevEdgeCopy.getEndNode() == r.getStartNode()) {
                    prevEdgeCopy.setSecondNextRelId(r.getEntityId());
                } else {
                    throw new IllegalStateException("Invalid state while updating relationship pointers");
                }
                // Update the timestamp and add the new copy back to the relationships
                if (addEdge) {
                    // Do I need to update the timestamp?
                    // prevEdgeCopy.setTimestamp(r.getTimestamp());
                    relLineage.put(prevEdgeId, prevEdgeCopy);
                    if (!prevEdge.isDiff()) {
                        prevEdgeCopy.unsetDiff();
                    }
                }

                // Set current's edge previous pointer
                r.setFirstPrevRelId(prevEdgeId);
            }
        }

        // Repeat the same symmetrically for the target node
        if (r.getStartNode() != r.getEndNode()) {
            Optional<InMemoryNeighbourhood> prevTargetEdge = neighbourhoodLineage.get(r.getEndNode(), r.getStartTimestamp());
            if (prevTargetEdge.isPresent()) {
                // Update it next pointer to the new edge if it's not the same as r
                long prevEdgeId = prevTargetEdge.get().getEntityId();
                if (prevEdgeId != r.getEntityId()) {
                    InMemoryRelationship prevEdge =
                            relLineage.get(prevEdgeId, r.getStartTimestamp()).get();
                    // Check if we just added this relationship with this timestamp
                    boolean addEdge = prevEdge.getStartTimestamp() != r.getStartTimestamp();
                    InMemoryRelationship prevEdgeCopy = (addEdge) ? prevEdge.copy() : prevEdge;
                    if (prevEdgeCopy.getStartNode() == r.getEndNode()) {
                        prevEdgeCopy.setFirstNextRelId(r.getEntityId());
                    } else if (prevEdgeCopy.getEndNode() == r.getEndNode()) {
                        prevEdgeCopy.setSecondNextRelId(r.getEntityId());
                    } else {
                        throw new IllegalStateException("Invalid state while updating relationship pointers");
                    }
                    // Update the timestamp and add the new copy back to the relationships
                    if (addEdge) {
                        // Do I need to update the timestamp?
                        // prevEdgeCopy.setTimestamp(r.getTimestamp());
                        relLineage.put(prevEdgeId, prevEdgeCopy);
                        if (!prevEdge.isDiff()) {
                            prevEdgeCopy.unsetDiff();
                        }
                    }
                    // Set current's edge previous pointer
                    r.setSecondPrevRelId(prevEdgeId);
                }
            }
        }
    }

    @Override
    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}
