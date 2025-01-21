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

public enum RelationshipDirection {
    INCOMING,
    OUTGOING,
    BOTH;

    public static RelationshipDirection getDirection(int type) {
        RelationshipDirection rd;
        switch (type) {
            case -1 : rd =  INCOMING;break;
            case 0 : rd = BOTH;break;
            case 1 : rd = OUTGOING;break;
            default : throw new RuntimeException(String.format("Unsupported type %d", type));
        }
        return rd;
    }
}
