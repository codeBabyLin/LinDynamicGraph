package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryRelationship;
import cn.Lin.EasyDynamicGraph.entities.Property;
import cn.Lin.EasyDynamicGraph.entities.RelationshipDirection;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.RelationshipStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class PersistentRelationStore implements RelationshipStore {


    private EntityVersionStore entityVersionStore;
    private Transformer transformer;
    private RelationStore relationStore;
    private PropertyVersionStore propertyVersionStore;

    public PersistentRelationStore(String path, Transformer transformer){
        //String labelStorePath = Paths.get(path,"labelStore").toString();
        //String propertyStorePath = Paths.get(path,"nodePropertyStore").toString();
        String entityStorePath = Paths.get(path,"relationTimeStore").toString();
        String relationStorePath = Paths.get(path,"relationStore").toString();
        String propertyStorePath = Paths.get(path,"relationPropertyStore").toString();

        this.transformer = transformer;
        this.entityVersionStore = new EntityVersionStore(entityStorePath,transformer);
        this.relationStore = new RelationStore(relationStorePath,transformer);
        this.propertyVersionStore = new PropertyVersionStore(propertyStorePath,transformer);
    }


    private void storeRelationProperties(long relId, List<Property> properties, long startTime, long endTime){
        for(Property property: properties){
            this.propertyVersionStore.updateProperty(relId,property.name(),startTime,endTime,property.value());
        }
    }

    @Override
    public void addRelationship(InMemoryRelationship rel) throws IOException {
        this.entityVersionStore.addEntity(rel.getEntityId(),rel.getStartTimestamp());
        this.entityVersionStore.deleteEntity(rel.getEntityId(),rel.getEndTimestamp());
        this.relationStore.addRelation(rel);
        storeRelationProperties(rel.getEntityId(),rel.getProperties(),rel.getStartTimestamp(),rel.getEndTimestamp());
    }

    @Override
    public void addRelationships(List<InMemoryRelationship> rels) throws IOException {
        for(InMemoryRelationship relationship: rels){
            addRelationship(relationship);
        }
    }

    @Override
    public Optional<InMemoryRelationship> getRelationship(long relId, long timestamp) throws IOException {

        long startTime = this.entityVersionStore.getEntityCreateVersion(relId);
        long endTime = this.entityVersionStore.getEntityDeleteVersion(relId);
        Optional<InMemoryRelationship> res = Optional.empty();
        if(startTime<=timestamp && timestamp<= endTime){
            InMemoryRelationship relationship = new InMemoryRelationship(relId,-1,-1,-1,-1);
            relationship.setStartTimestamp(startTime);
            relationship.setEndTimestamp(endTime);
            relationship.setStartNode(this.relationStore.getStartNode(relId));
            relationship.setEndNode(this.relationStore.getEndNode(relId));
            relationship.setType((int)this.relationStore.getType(relId));
            List<Property> properties = this.propertyVersionStore.getPropertiesByTime(relId,timestamp);
            for(Property property: properties){
                relationship.addProperty(property.name(),property.value());
            }
            res = Optional.of(relationship);

        }
        return res;
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
