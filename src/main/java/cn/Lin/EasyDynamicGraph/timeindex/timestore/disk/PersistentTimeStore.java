package cn.Lin.EasyDynamicGraph.timeindex.timestore.disk;

import cn.Lin.EasyDynamicGraph.entities.*;
import cn.Lin.EasyDynamicGraph.timeindex.SnapshotCreationPolicy;
import cn.Lin.EasyDynamicGraph.timeindex.timestore.TimeStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PersistentTimeStore implements TimeStore {

    private final SnapshotCreationPolicy policy;
    private final Map<String, Integer> namesToIds;
    private final Map<Integer, String> idsToNames;
    private final Path dataPath;
    private long updateCounter;




    private final SnapshotGraphStore graphStore;
    private long currentTimestamp;
    private final InMemoryGraph currentGraph;
    private final boolean storeSnaphots;



    public PersistentTimeStore(SnapshotCreationPolicy policy, Map<String, Integer> namesToIds,
                               Map<Integer, String> idsToNames,Path storePath) {
        this.policy = policy;
        this.idsToNames = idsToNames;
        this.namesToIds = namesToIds;
        this.updateCounter = 0;
        this.currentTimestamp = -1L;
        this.dataPath = storePath;
        this.graphStore = new SnapshotGraphStore(this.dataPath.toString());
        this.storeSnaphots = this.policy.storeSnapshots();
        this.currentGraph = this.storeSnaphots?InMemoryGraph.createGraph():null;
    }


    @Override
    public void addUpdate(InMemoryEntity entity) throws IOException {
        if(storeSnaphots){
            currentGraph.updateGraph(entity);
        }
        currentTimestamp = Math.max(entity.getStartTimestamp(),currentTimestamp);
        updateCounter++;
    }

    @Override
    public void takeSnapshot() throws IOException {
        if(policy.readyToTakeSnapshot(updateCounter)){
            graphStore.storeSnapshot(currentGraph,currentTimestamp);
            updateCounter = 0;
        }
    }

    @Override
    public void flushLog() throws IOException {

    }

    @Override
    public void flushIndexes() throws IOException {

    }

    @Override
    public void reset() {

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
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {
        return Optional.empty();
    }

    @Override
    public List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp) throws IOException {
        return null;
    }

    @Override
    public List<List<InMemoryRelationship>> getRelationships(long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp) throws IOException {
        return null;
    }

    @Override
    public List<List<InMemoryNode>> expand(long nodeId, RelationshipDirection direction, int hops, long startTime, long endTime, long timeStep) throws IOException {
        return null;
    }

    @Override
    public InMemoryGraph getWindow(long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public InMemoryGraph getGraph(long timestamp) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryGraph> getGraph(long startTime, long endTime, long timeStep) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryEntity> getDiff(long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public void shutdown() throws IOException {

    }
}
