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
package cn.Lin.EasyDynamicGraph.timeindex.timestore;

import java.io.IOException;
import cn.Lin.EasyDynamicGraph.HistoryStore;
import cn.Lin.EasyDynamicGraph.entities.InMemoryEntity;

public interface TimeStore extends HistoryStore {

    void addUpdate(InMemoryEntity entity) throws IOException;

    void takeSnapshot() throws IOException;

    void flushLog() throws IOException;

    void flushIndexes() throws IOException;

    void reset();

    interface FilterInterface {
        boolean apply(InMemoryEntity entity);
    }
}
