package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNeighbourhood;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.RelationshipStore;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.stream.Collectors;

public class DoubleListRelationshipStore implements RelationshipStore {

    // (K - V) => K: edgeId, V: edge mutations ordered by timestamp
    // The edges create a "temporal" linked list of neighbourhoods for source/target nodes.
    private final EnhancedTreeMap<Long, InMemoryRelationship> relLineage;
    // (K - V) => K: (source, target, relId), V: node's edge list mutations ordered by timestamp
    private final EnhancedTreeMap<Triple<Long, Long, Long>, InMemoryNeighbourhood> outRels;
    // (K - V) => K: (target, source, relId), V: node's edge list mutations ordered by timestamp
    private final EnhancedTreeMap<Triple<Long, Long, Long>, InMemoryNeighbourhood> inRels;

    private static final List<InMemoryRelationship> emptyRelList = new ArrayList<>();//List.of();
    private static final List<List<InMemoryRelationship>> emptyRelListList = new ArrayList<>();//List.of(List.of());
    private static final List<InMemoryNeighbourhood> emptyNeighbourhoodList = new ArrayList<>();//List.of();
    private static final List<List<InMemoryNeighbourhood>> emptyNeighbourhoodListList = new ArrayList<>();//List.of(List.of());

    public DoubleListRelationshipStore() {
        relLineage = new EnhancedTreeMap<>(Comparator.comparingLong(Long::longValue));
        inRels = new EnhancedTreeMap<>(new LongTripleComparator());
        outRels = new EnhancedTreeMap<>(new LongTripleComparator());
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) {
        InMemoryRelationship relClone = rel.copy();
        relLineage.put(relClone.getEntityId(), relClone);

        long sourceId = relClone.getStartNode();
        long targetId = relClone.getEndNode();
        outRels.put(
                ImmutableTriple.of(sourceId, targetId, relClone.getEntityId()),
                new InMemoryNeighbourhood(relClone.getEntityId(), relClone.getStartTimestamp(), relClone.isDeleted()));
        if (sourceId != targetId) {
            inRels.put(
                    ImmutableTriple.of(targetId, sourceId, relClone.getEntityId()),
                    new InMemoryNeighbourhood(
                            relClone.getEntityId(), relClone.getStartTimestamp(), relClone.isDeleted()));
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
        List<InMemoryNeighbourhood> inNeighbourhood = (direction == RelationshipDirection.OUTGOING)
                ? emptyNeighbourhoodList
                : getInNeighbourhood(nodeId, timestamp);
        List<InMemoryNeighbourhood> outNeighbourhood = (direction == RelationshipDirection.INCOMING)
                ? emptyNeighbourhoodList
                : getOutNeighbourhood(nodeId, timestamp);
        if (inNeighbourhood.isEmpty() && outNeighbourhood.isEmpty()) {
            return emptyRelList;
        }

        ArrayList<InMemoryRelationship> rels = new ArrayList<InMemoryRelationship>();
        if (direction != RelationshipDirection.OUTGOING) {
            addNeighbourhood(rels, inNeighbourhood, timestamp);
        }
        if (direction != RelationshipDirection.INCOMING) {
            addNeighbourhood(rels, outNeighbourhood, timestamp);
        }
        return rels;
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) {
        List<List<InMemoryNeighbourhood>> inNeighbourhoods = (direction == RelationshipDirection.OUTGOING)
                ? emptyNeighbourhoodListList
                : getInNeighbourhood(nodeId, startTime, endTime);
        List<List<InMemoryNeighbourhood>> outNeighbourhoods = (direction == RelationshipDirection.INCOMING)
                ? emptyNeighbourhoodListList
                : getOutNeighbourhood(nodeId, startTime, endTime);
        if (inNeighbourhoods.isEmpty() && outNeighbourhoods.isEmpty()) {
            return emptyRelListList;
        }

        // Get and sort all relationships by timestamp
        List<InMemoryNeighbourhood> neighbourhoods = new ArrayList<>();
        if (direction != RelationshipDirection.OUTGOING) {
            for (List<InMemoryNeighbourhood> n : inNeighbourhoods) {
                neighbourhoods.addAll(n);
            }
        }
        if (direction != RelationshipDirection.INCOMING) {
            for (List<InMemoryNeighbourhood> n : outNeighbourhoods) {
                neighbourhoods.addAll(n);
            }
        }
        neighbourhoods.sort(Comparator.comparing(InMemoryNeighbourhood::getStartTimestamp));

        // Go through the relationships and create the final result
        ArrayList<List<InMemoryRelationship>> rels = new ArrayList<List<InMemoryRelationship>>();
        long currentTime = neighbourhoods.get(0).getStartTimestamp();
        Map<Long, InMemoryRelationship> currentRels = new HashMap<>();
        for (InMemoryNeighbourhood n : neighbourhoods) {
            if (currentTime != n.getStartTimestamp() && !currentRels.isEmpty()) {
                rels.add(new ArrayList<>(currentRels.values()));
                currentTime = n.getStartTimestamp();
            }

            Optional<InMemoryRelationship> rel = getRelationship(n.getEntityId(), n.getStartTimestamp());
            assert (rel.isPresent());
            if (rel.get().isDeleted()) {
                currentRels.remove(n.getEntityId());
            } else {
                currentRels.put(n.getEntityId(), rel.get());
            }
        }
        if (!currentRels.isEmpty()) {
            rels.add(new ArrayList<>(currentRels.values()));
        }

        return rels;
    }

    private void addNeighbourhood(
            List<InMemoryRelationship> result, List<InMemoryNeighbourhood> neighbourhood, long timestamp) {
        for (InMemoryNeighbourhood n : neighbourhood) {
            if (!n.isDeleted()) {
                Optional<InMemoryRelationship> nextRel = relLineage.get(n.getEntityId(), timestamp);
                nextRel.ifPresent(result::add);
            }
        }
    }

    @Override
    public List<InMemoryRelationship> getAllRelationships(long timestamp) {
        return relLineage.getAll(timestamp);
    }

    @Override
    public void reset() {
        relLineage.reset();
        inRels.reset();
        outRels.reset();
    }

    private List<InMemoryNeighbourhood> getInNeighbourhood(long nodeId, long timestamp) {
        Triple<Long,Long,Long> fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        Triple<Long,Long,Long> toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return inRels.rangeScanByKey(fromKey, toKey, timestamp);
    }

    private List<List<InMemoryNeighbourhood>> getInNeighbourhood(long nodeId, long startTime, long endTime) {
        Triple<Long,Long,Long> fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        Triple<Long,Long,Long> toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return inRels.rangeScanByKeyAndTime(fromKey, toKey, startTime, endTime);
    }

    private List<InMemoryNeighbourhood> getOutNeighbourhood(long nodeId, long timestamp) {
        Triple<Long,Long,Long> fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        Triple<Long,Long,Long> toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return outRels.rangeScanByKey(fromKey, toKey, timestamp);
    }

    private List<List<InMemoryNeighbourhood>> getOutNeighbourhood(long nodeId, long startTime, long endTime) {
        Triple<Long,Long,Long> fromKey = ImmutableTriple.of(nodeId, -1L, -1L);
        Triple<Long,Long,Long> toKey = ImmutableTriple.of(nodeId, Long.MAX_VALUE, Long.MAX_VALUE);
        return outRels.rangeScanByKeyAndTime(fromKey, toKey, startTime, endTime);
    }

    static class LongTripleComparator implements Comparator<Triple<Long, Long, Long>> {
        @Override
        public int compare(final Triple<Long, Long, Long> lhs, final Triple<Long, Long, Long> rhs) {
            final int left = lhs.getLeft().compareTo(rhs.getLeft());
            if (left == 0) {
                final int middle = lhs.getMiddle().compareTo(rhs.getMiddle());
                if (middle == 0) {
                    return lhs.getRight().compareTo(rhs.getRight());
                }
                return middle;
            }
            return left;
        }
    }

    public int numberOfRelationships() {
        int result = 0;
        for (Map.Entry<Long,List<Pair<Long,InMemoryRelationship>>> e : relLineage.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    public int numberOfNeighbourhoodRecords() {
        int result = 0;
        for (Map.Entry<Triple<Long,Long,Long>,List<Pair<Long,InMemoryNeighbourhood>>> e : inRels.tree.entrySet()) {
            result += e.getValue().size();
        }
        for (Map.Entry<Triple<Long,Long,Long>,List<Pair<Long,InMemoryNeighbourhood>>> e : outRels.tree.entrySet()) {
            result += e.getValue().size();
        }
        return result;
    }

    @Override
    public void flushIndexes() {}

    @Override
    public void shutdown() {}

    @Override
    public void setDiffThreshold(int threshold) {
        throw new IllegalStateException("Implement this method");
    }
}