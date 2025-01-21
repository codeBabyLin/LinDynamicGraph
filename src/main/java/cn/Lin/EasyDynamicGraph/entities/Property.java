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

import cn.Lin.EasyDynamicGraph.entities.PropertyType;

public class Property{
    PropertyType type;
    String name;
    Object value;
    public Property(PropertyType type, String name, Object value){
      this.type = type;
      this.name = name;
      this.value =value;
    }

    public String name(){
        return this.name;
    }
    public Object value(){
        return this.value;
    }
    public PropertyType type(){
        return this.type;
    }

    public int getSize() {
        int size;
        switch (this.type) {
            case LONG: size = Long.BYTES;break;
            case INT : size =Integer.BYTES;break;
            case FLOAT :size = Float.BYTES;break;
            case DOUBLE : size =Double.BYTES;break;
            default : throw new UnsupportedOperationException(String.format("Type %s is not supported", type));
        }
        return size;
    }
}
