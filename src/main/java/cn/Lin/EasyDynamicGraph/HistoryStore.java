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
package cn.Lin.EasyDynamicGraph;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import cn.Lin.EasyDynamicGraph.entities.InMemoryEntity;
import cn.Lin.EasyDynamicGraph.entities.InMemoryGraph;
import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.entities.TemporalGraph;

public interface HistoryStore {
    // Point Queries
    Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException;

    List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException;

    Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException;

    List<InMemoryRelationship> getRelationship(long relId, long startTime, long endTime) throws IOException;

    List<InMemoryRelationship> getRelationships(long nodeId, RelationshipDirection direction, long timestamp)
            throws IOException;

    List<List<InMemoryRelationship>> getRelationships(
            long nodeId, RelationshipDirection direction, long startTime, long endTime) throws IOException;

    // Neighbourhood Queries
    List<InMemoryNode> expand(long nodeId, RelationshipDirection direction, int hops, long timestamp)
            throws IOException;

    List<List<InMemoryNode>> expand(
            long nodeId, RelationshipDirection direction, int hops, long startTime, long endTime, long timeStep)
            throws IOException;

    // Global Queries
    InMemoryGraph getWindow(long startTime, long endTime) throws IOException;

    InMemoryGraph getGraph(long timestamp) throws IOException;

    List<InMemoryGraph> getGraph(long startTime, long endTime, long timeStep) throws IOException;

    List<InMemoryEntity> getDiff(long startTime, long endTime) throws IOException;

    TemporalGraph getTemporalGraph(long startTime, long endTime) throws IOException;

    void shutdown() throws IOException;
}
