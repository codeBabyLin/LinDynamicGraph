package cn.Lin.EasyDynamicGraph.lineageindex.entitystores;

import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface RelationshipStore {
    void addRelationship(InMemoryRelationship rel) throws IOException;

    void addRelationships(List<InMemoryRelationship> rels) throws IOException;

    Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException;

    List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException;

    List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException;

    List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException;

    List<InMemoryRelationship> getAllRelationships(long timestamp) throws IOException;

    void reset();

    int numberOfRelationships() throws IOException;

    int numberOfNeighbourhoodRecords() throws IOException;

    void flushIndexes();

    void shutdown() throws IOException;

    void setDiffThreshold(int threshold);
}

