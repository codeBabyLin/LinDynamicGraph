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
package cn.Lin.EasyDynamicGraph.timeindex.timestore.memory;

import java.io.IOException;
import java.util.*;

import cn.Lin.EasyDynamicGraph.entities.InMemoryEntity;
import cn.Lin.EasyDynamicGraph.entities.InMemoryGraph;
import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.entities.TemporalGraph;
import cn.Lin.EasyDynamicGraph.timeindex.SnapshotCreationPolicy;
import cn.Lin.EasyDynamicGraph.timeindex.timestore.TimeStore;
import cn.Lin.EasyDynamicGraph.utils.IntCircularList;
import org.roaringbitmap.RoaringBitmap;

public class InMemoryTimeStore implements TimeStore {
    private final SnapshotCreationPolicy policy;
    private long updateCounter;
    private final List<InMemoryEntity> log;
    // Index the positions of the log based on timestamp
    private final TreeMap<Long, List<Integer>> timeIndex;
    // Index the stored snapshots
    private final TreeMap<Long, InMemoryGraph> snapshotIndex;
    private long currentTimestamp;
    private final InMemoryGraph currentGraph;

    // Thread local variables for graph retrieval
    private static final ThreadLocal<IntCircularList> bfsQueue = ThreadLocal.withInitial(IntCircularList::new);
    private final ThreadLocal<RoaringBitmap> visitedNodes = ThreadLocal.withInitial(RoaringBitmap::new);

    public InMemoryTimeStore(SnapshotCreationPolicy policy) {
        this.policy = policy;
        this.updateCounter = 0;
        this.currentTimestamp = -1L;
        this.log = new ArrayList<>();
        this.timeIndex = new TreeMap<>();
        this.snapshotIndex = new TreeMap<>();
        this.currentGraph = InMemoryGraph.createGraph();
    }

    public void addUpdate(InMemoryEntity entity) {
        // Log the update
        log.add(entity.copy());

        // Index its offset
        List<Integer> prevEntry = timeIndex.computeIfAbsent(entity.getStartTimestamp(), k -> new ArrayList<>());
        prevEntry.add(log.size() - 1);

        // Update the in-memory snapshot
        currentGraph.updateGraph(entity, false);

        // Increase the current timestamp and updates' counter
        currentTimestamp = Math.max(entity.getStartTimestamp(), currentTimestamp);
        updateCounter++;
    }

    /**
     * When calling this method, we assume that all updates up to @currentTimestamp
     * have been stored. Otherwise, the getGraph() method will not return a correct
     * result.
     */
    public void takeSnapshot() {
        // check if we have to create a snapshot
        if (policy.readyToTakeSnapshot(updateCounter)) {
            snapshotIndex.put(currentTimestamp, currentGraph.copy());
            updateCounter = 0;
        }
    }

    @Override
    public void flushLog() {}

    @Override
    public void flushIndexes() throws IOException {}

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) {
        InMemoryGraph graph =
                getGraphWithFilter(timestamp, e -> {
                    if(e instanceof  InMemoryNode){
                        InMemoryNode node = (InMemoryNode)e;
                        if(node.getEntityId()==nodeId) return true;
                    }
                    return false;
                });
        return graph.getNode((int) nodeId);
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) {
        FilterInterface filter = e -> {
            if(e instanceof  InMemoryNode){
                InMemoryNode node = (InMemoryNode)e;
                if(node.getEntityId()==nodeId) return true;
            }
            return false;
        };
        InMemoryGraph graph = getGraphWithFilter(startTime, filter);
        Optional<InMemoryNode> node = graph.getNode((int) nodeId);
        InMemoryNode actualNode;

        ArrayList<InMemoryNode> result = new ArrayList<InMemoryNode>();
        if (node.isPresent()) {
            actualNode = node.get().copy();
            result.add(actualNode);
        } else {
            actualNode = new InMemoryNode(nodeId, startTime);
        }

        SortedMap<Long,List<Integer>> updates = timeIndex.subMap(startTime + 1, endTime + 1);
        for (List<Integer> list : updates.values()) {
            for (Integer index : list) {
                InMemoryEntity update = log.get(index);
                if (filter.apply(update)) {
                    actualNode = actualNode.copy();
                    actualNode.merge(update);
                    result.add(actualNode);
                }
            }
        }

        return result;
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) {
        InMemoryGraph graph = getGraphWithFilter(
                timestamp, e -> {
                    if(e instanceof  InMemoryRelationship){
                        InMemoryRelationship node = (InMemoryRelationship)e;
                        if(node.getEntityId()==relId) return true;
                    }
                    return false;
                });
        return graph.getRelationship((int) relId);
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) {
        FilterInterface filter = e -> {
            if(e instanceof  InMemoryRelationship){
                InMemoryRelationship node = (InMemoryRelationship)e;
                if(node.getEntityId()==relId) return true;
            }
            return false;
        };
        InMemoryGraph graph = getGraphWithFilter(startTime, filter);
        Optional<InMemoryRelationship> rel = graph.getRelationship((int) relId);
        InMemoryRelationship actualRel;

        ArrayList<InMemoryRelationship> result = new ArrayList<InMemoryRelationship>();
        if (rel.isPresent()) {
            actualRel = rel.get().copy();
            result.add(actualRel);
        } else {
            actualRel = new InMemoryRelationship(relId, -1, -1, -1, startTime);
        }

        SortedMap<Long,List<Integer>> updates = timeIndex.subMap(startTime + 1, endTime + 1);
        for (List<Integer> list : updates.values()) {
            for (Integer index : list) {
                InMemoryEntity update = log.get(index);
                if (filter.apply(update)) {
                    actualRel = actualRel.copy();
                    actualRel.merge(update);
                    result.add(actualRel);
                }
            }
        }

        return result;
    }

    @Override
    public InMemoryGraph getWindow(long startTime, long endTime) throws IOException {
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException {
        throw new IllegalStateException("Implement this method...");
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp) {
        InMemoryGraph graph = getGraphWithFilter(timestamp, e -> {
            if (e instanceof InMemoryNode ) {
                InMemoryNode node = (InMemoryNode)e;

                return node.getEntityId() == nodeId;
            } else{
                if(e instanceof InMemoryRelationship){
                    InMemoryRelationship rel = (InMemoryRelationship)e;
                    return ((rel.getStartNode() == nodeId
                            && (direction == RelationshipDirection.BOTH
                            || direction == RelationshipDirection.OUTGOING))
                            || (rel.getEndNode() == nodeId
                            && (direction == RelationshipDirection.BOTH
                            || direction == RelationshipDirection.INCOMING)));
                }
            }
            return false;
        }
        );

        return graph.getRelationships((int) nodeId, direction);
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) {
        ArrayList<List<InMemoryRelationship>> result = new ArrayList<List<InMemoryRelationship>>();

        FilterInterface filter = e -> {
            if (e instanceof InMemoryNode ) {
                InMemoryNode node = (InMemoryNode)e;

                return node.getEntityId() == nodeId;
            } else{
                if(e instanceof InMemoryRelationship){
                    InMemoryRelationship rel = (InMemoryRelationship)e;
                    return ((rel.getStartNode() == nodeId
                            && (direction == RelationshipDirection.BOTH
                            || direction == RelationshipDirection.OUTGOING))
                            || (rel.getEndNode() == nodeId
                            && (direction == RelationshipDirection.BOTH
                            || direction == RelationshipDirection.INCOMING)));
                }
            }
            return false;
        };

        long currentTime = startTime;
        InMemoryGraph graph = getGraphWithFilter(startTime, filter);

        SortedMap<Long,List<Integer>> updates = timeIndex.subMap(startTime + 1, endTime + 1);
        for (List<Integer> list : updates.values()) {
            for (Integer index : list) {
                InMemoryEntity update = log.get(index);
                if (filter.apply(update)) {
                    if (currentTime != update.getStartTimestamp()) {
                        result.add(graph.getRelationships((int) nodeId, direction));
                        currentTime = update.getStartTimestamp();
                    }
                    graph.updateGraph(update, true);
                }
            }
        }
        result.add(graph.getRelationships((int) nodeId, direction));

        return result;
    }

    @Override
    public List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp) {
        if (direction != RelationshipDirection.OUTGOING) {
            throw new UnsupportedOperationException("Supporting only outgoing edges now");
        }

        ArrayList<InMemoryNode> result = new ArrayList<InMemoryNode>();

        InMemoryGraph graph = getGraph(timestamp);

        RoaringBitmap bitmap = visitedNodes.get();
        IntCircularList queue = bfsQueue.get();
        queue.clear();

        queue.add((int) nodeId);
        for (int i = 0; i < hops; i++) {

            bitmap.clear();
            int queueSize = queue.size();
            for (int j = 0; j < queueSize; ++j) {
                int currentNode = queue.poll();
                List<InMemoryRelationship> rels = graph.getRelationships(currentNode, direction);
                for (InMemoryRelationship r : rels) {
                    // Outgoing edge
                    if (r.getStartNode() == currentNode) {

                        // Get target node
                        int targetId = (int) r.getEndNode();
                        Optional<InMemoryNode> targetNode = graph.getNode(targetId);
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
            long nodeId, RelationshipDirection direction, int hops, long startTime, long endTime, long timeStep) {
        ArrayList<List<InMemoryNode>> result = new ArrayList<List<InMemoryNode>>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(expand(nodeId, direction, hops, time));
        }
        return result;
    }

    @Override
    public InMemoryGraph getGraph(long timestamp) {
        return getGraphWithFilter(timestamp, e -> true);
    }

    @Override
    public List<InMemoryGraph> getGraph(long startTime, long endTime, long timeStep) {
        ArrayList<InMemoryGraph> result = new ArrayList<InMemoryGraph>();
        for (long time = startTime; time <= endTime; time += timeStep) {
            result.add(getGraph(time));
        }
        return result;
    }

    @Override
    public List<InMemoryEntity> getDiff(long startTime, long endTime) {
        Integer startPosition = timeIndex.get(startTime).get(0);
        Integer endPosition = timeIndex.get(endTime).get(0);
        return log.subList(startPosition, endPosition);
    }

    public void reset() {
        updateCounter = 0;
        currentTimestamp = -1L;
        log.clear();
        timeIndex.clear();
        snapshotIndex.clear();
        currentGraph.clear();
    }

    private InMemoryGraph getGraphWithFilter(long timestamp, FilterInterface filter) {
        InMemoryGraph graph;
        long prevTimestamp = -1L;

        // First try to get the most recent snapshot
        Map.Entry<Long,InMemoryGraph> entry = snapshotIndex.floorEntry(timestamp);
        if (entry != null) {
            prevTimestamp = entry.getKey();
            graph = entry.getValue();
        } else {
            graph = InMemoryGraph.createGraph();
        }

        // Now mutate the graph with any missing updates
        // Perform updates in a forward fashion
        if (prevTimestamp < timestamp) {

            // Create a copy of the graph to maintain the stored snapshot
            graph = graph.copy();

            SortedMap<Long,List<Integer>> updates = timeIndex.subMap(prevTimestamp, timestamp + 1);
            for (List<Integer> list : updates.values()) {
                for (Integer index : list) {
                    InMemoryEntity update = log.get(index);
                    if (filter.apply(update)) {
                        graph.updateGraph(update, true);
                    }
                }
            }
        }
        return graph;
    }

    @Override
    public void shutdown() {}
}
