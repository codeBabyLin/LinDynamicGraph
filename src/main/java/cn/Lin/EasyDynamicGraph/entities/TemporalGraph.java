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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;

public class TemporalGraph {
    private final Map<Integer, List<InMemoryNode>> nodes;
    private final Map<Integer, List<InMemoryRelationship>> relationships;

    public TemporalGraph() {
        nodes = Maps.mutable.empty();
        relationships = Maps.mutable.empty();
    }

    public void initialize(InMemoryGraph graph) {
        ObjectArray.ObjectArrayIterator nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode node = (InMemoryNode) nodeIterator.next();
            addNode(node);
        }
        ObjectArray.ObjectArrayIterator relIterator = graph.getRelationshipMap().iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
            addRelationship(rel);
        }
    }

    public void addNode(InMemoryNode node) {
        int nodeId = (int) node.getEntityId();

        List<InMemoryNode> prevNodes = nodes.get(nodeId);
        if (prevNodes != null) {
            node = rebuildNodeHistory(prevNodes, node);
            setNodeEndTimestamp(prevNodes, node.getStartTimestamp());
        } else {
            prevNodes = Lists.mutable.empty();
            nodes.put(nodeId, prevNodes);
        }
        prevNodes.add(node.copy());
    }

    private InMemoryNode rebuildNodeHistory(List<InMemoryNode> nodes, InMemoryNode current) {
        if (nodes != null && nodes.get(nodes.size() - 1).getEndTimestamp() == Long.MAX_VALUE) {
            InMemoryNode prev = nodes.get(nodes.size() - 1).copy();
            prev.merge(current);
            current = prev;
        }
        return current;
    }

    private void setNodeEndTimestamp(List<InMemoryNode> nodes, long timestamp) {
        if (nodes != null && nodes.get(nodes.size() - 1).getEndTimestamp() == Long.MAX_VALUE) {
            nodes.get(nodes.size() - 1).setEndTimestamp(timestamp);
        }
    }

    public void deleteNode(int nodeId, long timestamp) {
        List<InMemoryNode> prevNodes = nodes.get(nodeId);
        setNodeEndTimestamp(prevNodes, timestamp);
    }

    public void addRelationship(InMemoryRelationship rel) {
        int relId = (int) rel.getEntityId();

        List<InMemoryRelationship> prevRels = relationships.get(relId);
        if (prevRels != null) {
            rel = rebuildRelationshipHistory(prevRels, rel);
            setRelationshipEndTimestamp(prevRels, rel.getStartTimestamp());
        } else {
            prevRels = Lists.mutable.empty();
            relationships.put(relId, prevRels);
        }
        prevRels.add(rel.copy());
    }

    public void deleteRelationship(int relId, long timestamp) {
        List<InMemoryRelationship> prevRels = relationships.get(relId);
        setRelationshipEndTimestamp(prevRels, timestamp);
    }

    private InMemoryRelationship rebuildRelationshipHistory(
            List<InMemoryRelationship> rels, InMemoryRelationship current) {
        if (rels != null && rels.get(rels.size() - 1).getEndTimestamp() == Long.MAX_VALUE) {
            InMemoryRelationship prev = rels.get(rels.size() - 1).copy();
            prev.merge(current);
            current = prev;
        }
        return current;
    }

    private void setRelationshipEndTimestamp(List<InMemoryRelationship> rels, long timestamp) {
        if (rels != null && rels.get(rels.size() - 1).getEndTimestamp() == Long.MAX_VALUE) {
            rels.get(rels.size() - 1).setEndTimestamp(timestamp);
        }
    }

    public void setEndTime(long endTime) {
        for (List<InMemoryNode> nodeList : nodes.values()) {
            InMemoryNode node = (!nodeList.isEmpty()) ? nodeList.get(nodeList.size() - 1) : null;
            if (node != null && node.getEndTimestamp() == Long.MAX_VALUE) {
                node.setEndTimestamp(endTime);
            }
        }
        for (List<InMemoryRelationship> relList : relationships.values()) {
            InMemoryRelationship rel = (!relList.isEmpty()) ? relList.get(relList.size() - 1) : null;
            if (rel != null && rel.getEndTimestamp() == Long.MAX_VALUE) {
                rel.setEndTimestamp(endTime);
            }
        }
    }

    public List<InMemoryNode> getNodes() {
        return nodes.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    public List<InMemoryRelationship> getRelationships() {
        return relationships.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    public void graphUnion(InMemoryGraph graph) {
        long maxTimestamp = Long.MIN_VALUE;

        ObjectArray.ObjectArrayIterator nodeIterator = graph.getNodeMap().iterator();
        while (nodeIterator.hasNext()) {
            InMemoryNode node = (InMemoryNode) nodeIterator.next();
            int nodeId = (int) node.getEntityId();
            maxTimestamp = Math.max(maxTimestamp, node.getStartTimestamp());
            boolean nodeExists = this.nodes.containsKey(nodeId);
            if (nodeExists && this.nodes.get(nodeId).contains(node)) {
                continue;
            }

            addNode(node);
        }

        ObjectArray.ObjectArrayIterator relIterator = graph.getRelationshipMap().iterator();
        while (relIterator.hasNext()) {
            InMemoryRelationship rel = (InMemoryRelationship) relIterator.next();
            int relId = (int) rel.getEntityId();
            maxTimestamp = Math.max(maxTimestamp, rel.getStartTimestamp());
            boolean relExists = this.relationships.containsKey(relId);
            if (relExists && this.relationships.get(relId).contains(rel)) {
                continue;
            }

            addRelationship(rel);
        }

        // Delete non existent nodes/relationship
        for (Map.Entry<Integer,List<InMemoryNode>> entry : this.nodes.entrySet()) {
            int nodeId = entry.getKey();
            deleteNode(nodeId, maxTimestamp);
        }
        for (Map.Entry<Integer,List<InMemoryRelationship>> entry : this.relationships.entrySet()) {
            int relId = entry.getKey();
            deleteRelationship(relId, maxTimestamp);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final TemporalGraph other = (TemporalGraph) obj;
        if (this.nodes.size() != other.nodes.size() || this.relationships.size() != other.relationships.size()) {
            return false;
        }

        for (Map.Entry<Integer,List<InMemoryNode>> entry : this.nodes.entrySet()) {
            if (!other.nodes.containsKey(entry.getKey())) {
                return false;
            }
            List<InMemoryNode> nodeList = entry.getValue();
            List<InMemoryNode> otherNodeList = other.nodes.get(entry.getKey());
            if (!nodeList.equals(otherNodeList)) {
                return false;
            }
        }

        for (Map.Entry<Integer,List<InMemoryRelationship>> entry : this.relationships.entrySet()) {
            if (!other.relationships.containsKey(entry.getKey())) {
                return false;
            }
            List<InMemoryRelationship> relList = entry.getValue();
            List<InMemoryRelationship> otherRelList = other.relationships.get(entry.getKey());
            if (!relList.equals(otherRelList)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodes) ^ Objects.hashCode(relationships);
    }
}
