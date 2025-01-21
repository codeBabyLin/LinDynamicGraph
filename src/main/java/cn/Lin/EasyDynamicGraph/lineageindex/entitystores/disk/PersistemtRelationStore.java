package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.RelationshipStore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class PersistemtRelationStore implements RelationshipStore {
    @Override
    public void addRelationship(InMemoryRelationship rel) throws IOException {

    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {

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
    public List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException {
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public int numberOfRelationships() throws IOException {
        return 0;
    }

    @Override
    public int numberOfNeighbourhoodRecords() throws IOException {
        return 0;
    }

    @Override
    public void flushIndexes() {

    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void setDiffThreshold(int threshold) {

    }
}
