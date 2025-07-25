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

public class InMemoryEntityFactory {
    private InMemoryEntityFactory() {}

    public static InMemoryEntity newEntity(InMemoryEntity entity) {
        InMemoryEntity result;
        if (entity instanceof InMemoryNode) {
            result = new InMemoryNode(entity.getEntityId(), 0);
        } else if (entity instanceof InMemoryRelationshipV2) {
            result = new InMemoryRelationshipV2(entity.getEntityId(), 0, 0, 0, 0);
        } else if (entity instanceof InMemoryRelationship) {
            result = new InMemoryRelationship(entity.getEntityId(), 0, 0, 0, 0);
        } else if (entity instanceof InMemoryNeighbourhood) {
            result = new InMemoryNeighbourhood(entity.getEntityId(), 0, false);
        } else {
            throw new IllegalArgumentException("Unsupported type");
        }
        return result;
    }
}
