/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.Lin.EasyDynamicGraph.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.eclipse.collections.api.iterator.MutableIntIterator;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import cn.Lin.EasyDynamicGraph.utils.DBUtils;
import cn.Lin.EasyDynamicGraph.utils.ThreadPool;

public class InMemoryGraph {
    public static final MutableIntList EMPTY_LIST = IntLists.mutable.empty();

    private ObjectArray<InMemoryNode> nodes;
    private ObjectArray<InMemoryRelationship> relationships;
    private ObjectArray<MutableIntList> inRelationships;
    private ObjectArray<MutableIntList> outRelationships;
    private final boolean isCoW;

    private InMemoryGraph(boolean isCoW) {
        this.isCoW = isCoW;
    }

    public static InMemoryGraph createGraph() {
        return createGraph(DBUtils.nodeCapacity, DBUtils.relCapacity, false);
    }

    public static InMemoryGraph createGraph(int nodeCapacity, int relCapacity, boolean isCow) {
        InMemoryGraph graph = new InMemoryGraph(isCow);
        graph.nodes = new ObjectArray<>(nodeCapacity);
        graph.relationships = new ObjectArray<>(relCapacity);
        graph.inRelationships = new ObjectArray<>(nodeCapacity);
        graph.outRelationships = new ObjectArray<>(nodeCapacity);
        return graph;
    }

    public InMemoryGraph copy() {
        InMemoryGraph graphCopy = createGraph(nodes.size(), relationships.size(), false);
        ObjectArray.ObjectArrayIterator nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode n = (InMemoryNode)nodeIterator.next();
            //InMemoryNode node = (InMemoryNode) n;
            graphCopy.updateNode(n.copy());
        }
        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship r = (InMemoryRelationship)relIterator.next();
            graphCopy.updateRelationship(r.copy());
        }
        return graphCopy;
    }

    public InMemoryGraph parallelCopy() {
        InMemoryGraph graphCopy = createGraph(nodes.size(), relationships.size(), false);

        createCopyTasks(nodes, relationships, graphCopy, true, true);
        return graphCopy;
    }

    public InMemoryGraph parallelCopyWithoutObjectCreation(boolean updateNeighbourhood) {
        InMemoryGraph graphCopy = createGraph(nodes.size(), relationships.size(), true);

        createCopyTasks(nodes, relationships, graphCopy, updateNeighbourhood, false);
        return graphCopy;
    }

    public InMemoryGraph copyWithoutNeighbourhood() {
        InMemoryGraph graphCopy = createGraph(nodes.size(), relationships.size(), false);
        ObjectArray.ObjectArrayIterator nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode n =(InMemoryNode) nodeIterator.next();
            graphCopy.nodes.put((int) n.getEntityId(), n.copy());
        }
        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship r = (InMemoryRelationship) relIterator.next();
            graphCopy.relationships.put((int) r.getEntityId(), r.copy());
        }
        return graphCopy;
    }

    private static void createCopyTasks(
            ObjectArray<InMemoryNode> nodes,
            ObjectArray<InMemoryRelationship> relationships,
            InMemoryGraph graph,
            boolean updateNeighbourhood,
            boolean copyObjects) {
        ThreadPool threadPool = ThreadPool.getInstance();
        List<Callable<Boolean>> callableTasks = new ArrayList<>();
        final int numberOfWorkers = 4; // has to be a power of two

        for (int i = 0; i < numberOfWorkers; ++i) {
            int nodeStep = nodes.maxSize() / numberOfWorkers;
            int finalI = i;
            int startIndex = finalI * nodeStep;
            int endIndex = (finalI == numberOfWorkers - 1) ? nodes.maxSize() : (finalI + 1) * nodeStep;
            callableTasks.add(() -> {
                Object[] nodeArray = nodes.values();
                for (int index = startIndex; index < endIndex; index++) {
                    if (nodeArray[index] != null) {
                        InMemoryNode node = (InMemoryNode) nodeArray[index];
                        int nodeId = (int) node.getEntityId();
                        InMemoryNode newNode = (copyObjects) ? node.copy() : node;
                        graph.nodes.put(nodeId, newNode);
                    }
                }

                // todo: partition updates
                ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
                while (relIterator.hasNext()) {
                    InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
                    int relId = (int) rel.getEntityId();
                    int startNode = (int) rel.getStartNode();
                    int endNode = (int) rel.getEndNode();
                    InMemoryRelationship newRel = (copyObjects) ? rel.copy() : rel;
                    if ((relId & (numberOfWorkers - 1)) == finalI) {
                        graph.relationships.put(relId, newRel);
                    }

                    if (updateNeighbourhood) {
                        if ((startNode & (numberOfWorkers - 1)) == finalI) {
                            if (!graph.outRelationships.contains(startNode)) {
                                graph.outRelationships.put(startNode, IntLists.mutable.empty());
                            }
                            MutableIntList outEntry = graph.outRelationships.get(startNode); // todo: use locks to mutate the lists
                            outEntry.add(relId);
                        }
                        if ((endNode & (numberOfWorkers - 1)) == finalI) {
                            if (!graph.inRelationships.contains(endNode)) {
                                graph.inRelationships.put(endNode, IntLists.mutable.empty());
                            }
                            MutableIntList inEntry = graph.inRelationships.get(endNode); // todo: use locks to mutate the lists
                            inEntry.add(relId);
                        }
                    }
                }
                return true;
            });
        }

        try {
            List<Future<Boolean>> futures = threadPool.invokeAll(callableTasks);
            for (Future f : futures) {
                f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateGraph(InMemoryEntity entity) {
        updateGraph(entity, true);
    }

    public void updateGraph(InMemoryEntity entity, boolean updateNeighbourhood) {
        if (entity instanceof InMemoryNode ) {
            InMemoryNode node = (InMemoryNode)entity;
            updateNode(node);
        } else if (entity instanceof InMemoryRelationship ) {
            InMemoryRelationship rel = (InMemoryRelationship) entity;
            updateRelationship(rel, updateNeighbourhood);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported entity %s", entity.toString()));
        }
    }

    public void updateNode(InMemoryNode node) {
        int nodeId = (int) node.getEntityId();
        if (node.isDeleted()) {
            nodes.remove(nodeId);
        } else {

            InMemoryNode previousVersion = nodes.get(nodeId);
            if (previousVersion == null) {
                previousVersion = node.copy();
                nodes.put(nodeId, previousVersion);
            } else {
                if (!isCoW) {
                    previousVersion.merge(node);
                } else {
                    previousVersion = previousVersion.copy();
                    previousVersion.merge(node);
                    nodes.put(nodeId, previousVersion);
                }
            }
        }
    }

    public void updateRelationship(InMemoryRelationship rel) {
        updateRelationship(rel, true);
    }

    public void updateRelationship(InMemoryRelationship rel, boolean updateNeighbourhood) {
        int relId = (int) rel.getEntityId();
        int startNode = (int) rel.getStartNode();
        int endNode = (int) rel.getEndNode();
        if (rel.isDeleted()) {
            relationships.remove(relId);

            if (updateNeighbourhood) {
                // Remove in-out references
                MutableIntList outEntry = outRelationships.get(startNode);
                outEntry.remove(relId);
                MutableIntList inEntry = inRelationships.get(endNode);
                inEntry.remove(relId);
            }
        } else {

            InMemoryRelationship previousVersion = relationships.get(relId);
            if (previousVersion == null) {
                previousVersion = rel.copy();
                relationships.put(relId, previousVersion);

                if (updateNeighbourhood) {
                    // Update in-out references
                    if (!outRelationships.contains(startNode)) {
                        outRelationships.put(startNode, IntLists.mutable.empty());
                    }
                    MutableIntList outEntry = outRelationships.get(startNode);
                    outEntry.add(relId);

                    if (!inRelationships.contains(endNode)) {
                        inRelationships.put(endNode, IntLists.mutable.empty());
                    }
                    MutableIntList inEntry = inRelationships.get(endNode);
                    inEntry.add(relId);
                }
            } else {
                // We assume a relationship cannot change source/target and requires
                // deletion and re-insertion
                if (!isCoW) {
                    previousVersion.merge(rel);
                } else {
                    previousVersion = previousVersion.copy();
                    previousVersion.merge(rel);
                    relationships.put(relId, previousVersion);
                }
            }
        }
    }

    public List<InMemoryNode> getNodes() {
        return nodes.valuesAsList();
    }

    public ObjectArray<InMemoryNode> getNodeMap() {
        return nodes;
    }

    public List<InMemoryRelationship> getRelationships() {
        return relationships.valuesAsList();
    }

    public ObjectArray<InMemoryRelationship> getRelationshipMap() {
        return relationships;
    }

    public Optional<InMemoryNode> getNode(int nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public Optional<InMemoryRelationship> getRelationship(int relId) {
        return Optional.ofNullable(relationships.get(relId));
    }

    public int getRelationshipsCount(int nodeId, RelationshipDirection direction) {
        int count = 0;
        if (direction == RelationshipDirection.INCOMING || direction == RelationshipDirection.BOTH) {
            MutableIntList inEntry = inRelationships.get(nodeId);
            if (inEntry != null) {
                count += inEntry.size();
            }
        }
        if (direction == RelationshipDirection.OUTGOING || direction == RelationshipDirection.BOTH) {
            MutableIntList outEntry = outRelationships.get(nodeId);
            if (outEntry != null) {
                count += outEntry.size();
            }
        }

        return count;
    }

    public List<InMemoryRelationship> getRelationships(int nodeId, RelationshipDirection direction) {
        ArrayList<InMemoryRelationship> result = new ArrayList<InMemoryRelationship>();

        if (direction == RelationshipDirection.INCOMING || direction == RelationshipDirection.BOTH) {
            MutableIntList inEntry = inRelationships.get(nodeId);
            if (inEntry != null) {
                MutableIntIterator iterator = inEntry.intIterator();
                while (iterator.hasNext()) {
                    int relId = iterator.next();
                    result.add(relationships.get(relId));
                }
            }
        }
        if (direction == RelationshipDirection.OUTGOING || direction == RelationshipDirection.BOTH) {
            MutableIntList outEntry = outRelationships.get(nodeId);
            if (outEntry != null) {
                MutableIntIterator iterator = outEntry.intIterator();
                while (iterator.hasNext()) {
                    int relId = iterator.next();
                    result.add(relationships.get(relId));
                }
            }
        }

        return result;
    }

    public MutableIntList getIncomingRelationships(int nodeId) {
        MutableIntList inEntries = inRelationships.get(nodeId);
        return (inEntries != null) ? inEntries : EMPTY_LIST;
    }

    public MutableIntList getOutgoingRelationships(int nodeId) {
        MutableIntList outEntries = outRelationships.get(nodeId);
        return (outEntries != null) ? outEntries : EMPTY_LIST;
    }

    public void rebuildEdgeLists() {
        inRelationships.clear();
        outRelationships.clear();
        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
            int relId = (int) rel.getEntityId();
            int startNode = (int) rel.getStartNode();
            int endNode = (int) rel.getEndNode();

            if (!outRelationships.contains(startNode)) {
                outRelationships.put(startNode, IntLists.mutable.empty());
            }
            MutableIntList outEntry = outRelationships.get(startNode);
            outEntry.add(relId);

            if (!inRelationships.contains(endNode)) {
                inRelationships.put(endNode, IntLists.mutable.empty());
            }
            MutableIntList inEntry = inRelationships.get(endNode);
            inEntry.add(relId);
        }
    }

    public void clear() {
        nodes.clear();
        relationships.clear();
    }

    public long getSize() {
        long size = 0L;
        ObjectArray.ObjectArrayIterator nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode n = (InMemoryNode) nodeIterator.next();
            size += n.getSize();
        }
        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship r =(InMemoryRelationship) relIterator.next();
            size += r.getSize();
        }
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final InMemoryGraph other = (InMemoryGraph) obj;
        if (this.nodes.size() != other.nodes.size()
                || this.relationships.size() != other.relationships.size()
                || this.inRelationships.size() != other.inRelationships.size()
                || this.outRelationships.size() != other.outRelationships.size()) {
            return false;
        }

        if (!this.nodes.equals(other.nodes)
                || !this.relationships.equals(other.relationships)
                || !this.outRelationships.equals(other.outRelationships)
                || !this.inRelationships.equals(other.inRelationships)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodes)
                ^ Objects.hashCode(relationships)
                ^ Objects.hashCode(inRelationships)
                ^ Objects.hashCode(outRelationships);
    }

    public void isConsistent() {
        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
            if (!nodes.contains((int) rel.getStartNode())) {
                throw new IllegalStateException(String.format(
                        "Relationship %d has an invalid source %d", rel.getEntityId(), rel.getStartNode()));
            }
            if (!nodes.contains((int) rel.getEndNode())) {
                throw new IllegalStateException(
                        String.format("Relationship %d has an invalid source %d", rel.getEntityId(), rel.getEndNode()));
            }
        }
    }

    public InMemoryGraph filterByApplicationTime(long startTime, long endTime) {
        InMemoryGraph newGraph = createGraph(nodes.size(), relationships.size(), false);
        ObjectArray.ObjectArrayIterator nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode node = (InMemoryNode) nodeIterator.next();
            if (isValid(node, startTime, endTime)) {
                newGraph.updateNode(node);
            }
        }

        ObjectArray.ObjectArrayIterator relIterator = relationships.iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
            int source = (int) rel.getStartNode();
            int dest = (int) rel.getEndNode();
            if (newGraph.nodes.contains(source) && newGraph.nodes.contains(dest) && isValid(rel, startTime, endTime)) {
                newGraph.updateRelationship(rel);
            }
        }

        return newGraph;
    }

    private boolean isValid(InMemoryEntity entity, long startTime, long endTime) {
        // Use transactional time by default
        long applicationTimeStart = entity.getStartTimestamp();
        long applicationTimeEnd = entity.getEndTimestamp();
        for (Property p : entity.getProperties()) {
            if (p.name().equals("applicationStartTime")) {
                applicationTimeStart = getValue(p.value());
            }
            if (p.name().equals("applicationEndTime")) {
                applicationTimeEnd = getValue(p.value());
            }
        }

        return applicationTimeStart >= startTime && applicationTimeEnd <= endTime;
    }

    private long getValue(Object obj) {
        if (obj instanceof Integer) {
            Integer i = (Integer) obj;
            return i;
        } else if (obj instanceof Long ) {
            Long l = (Long)obj;
            return l;
        } else {
            throw new IllegalStateException(String.format("Unsupported type %s", obj.toString()));
        }
    }

    public boolean containsNode(int nodeId) {
        return nodes.contains(nodeId);
    }
}
