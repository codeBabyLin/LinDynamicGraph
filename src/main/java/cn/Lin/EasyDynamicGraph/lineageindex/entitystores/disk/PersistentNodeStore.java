package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.disk;

import cn.Lin.EasyDynamicGraph.entities.InMemoryNode;
import cn.Lin.EasyDynamicGraph.entities.Property;
import cn.Lin.EasyDynamicGraph.graphdb.persistent.Transformer;
import cn.Lin.EasyDynamicGraph.lineageindex.entitystores.NodeStore;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class PersistentNodeStore implements NodeStore {

    PropertyVersionStore propertyVersionStore;
    LabelVersionStore labelVersionStore;
    EntityVersionStore entityVersionStore;
    Transformer transformer;
    public PersistentNodeStore(String path, Transformer transformer){
        String labelStorePath = Paths.get(path,"labelStore").toString();
        String propertyStorePath = Paths.get(path,"nodePropertyStore").toString();
        String entityStorePath = Paths.get(path,"nodeTimeStore").toString();

        this.transformer = transformer;
        this.propertyVersionStore = new PropertyVersionStore(propertyStorePath,transformer);
        this.labelVersionStore = new LabelVersionStore(labelStorePath,transformer);
        this.entityVersionStore = new EntityVersionStore(entityStorePath,transformer);
    }


    private void storeNodeLables(long nodeId,List<String> lables,long startTime,long endTime){
        for(String label: lables){
            this.labelVersionStore.addLabel(nodeId,startTime,endTime,label);
        }
    }
    private void storeNodeProperties(long nodeId, List<Property> properties,long startTime, long endTime){
        for(Property property: properties){
            this.propertyVersionStore.updateProperty(nodeId,property.name(),startTime,endTime,property.value());
        }
    }

    @Override
    public void addNodes(List<InMemoryNode> nodes) throws IOException {
        for(InMemoryNode node: nodes){
            this.entityVersionStore.addEntity(node.getEntityId(),node.getStartTimestamp());//time stamp
           storeNodeLables(node.getEntityId(),node.getLabels(),node.getStartTimestamp(),node.getEndTimestamp());
           storeNodeProperties(node.getEntityId(),node.getProperties(),node.getStartTimestamp(),node.getEndTimestamp());
        }

    }

    @Override
    public Optional<InMemoryNode> getNode(long nodeId, long timestamp) throws IOException {
        long startTime = this.entityVersionStore.getEntityCreateVersion(nodeId);
        long endTime = this.entityVersionStore.getEntityDeleteVersion(nodeId);
        Optional<InMemoryNode> res = Optional.empty();
        if(startTime<=timestamp && timestamp<= endTime){
            InMemoryNode resNode = new InMemoryNode(nodeId,startTime);
            resNode.setEndTimestamp(endTime);
            String [] lables = this.labelVersionStore.getLabelsBytime(nodeId,timestamp);
            for(String label: lables){
                resNode.addLabel(label);
            }
            List<Property> properties = this.propertyVersionStore.getPropertiesByTime(nodeId,timestamp);
            for(Property property: properties){
                resNode.addProperty(property.name(),property.value());
            }
            res = Optional.of(resNode);

        }
        return res;
    }

    @Override
    public List<InMemoryNode> getNode(long nodeId, long startTime, long endTime) throws IOException {
        return null;
    }

    @Override
    public List<InMemoryNode> getAllNodes(long timestamp) throws IOException {

        ArrayList<InMemoryNode> nodes = new ArrayList<>();
        Iterator<Long> iter = this.entityVersionStore.AllEntitiesByVersion(timestamp);

        ArrayList<InMemoryNode> res = new ArrayList<>();

        while(iter.hasNext()){
            long nodeId = iter.next();
            long startTime = this.entityVersionStore.getEntityCreateVersion(nodeId);
            long endTime = this.entityVersionStore.getEntityDeleteVersion(nodeId);
            InMemoryNode resNode = new InMemoryNode(nodeId,startTime);
            resNode.setEndTimestamp(endTime);
            String [] lables = this.labelVersionStore.getLabelsBytime(nodeId,timestamp);
            for(String label: lables){
                resNode.addLabel(label);
            }
            List<Property> properties = this.propertyVersionStore.getPropertiesByTime(nodeId,timestamp);
            for(Property property: properties){
                resNode.addProperty(property.name(),property.value());
            }
            res.add(resNode);
        }
        return res;
    }

    @Override
    public void flushIndex() {

    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown() throws IOException {

    }

    @Override
    public void setDiffThreshold(int threshold) {

    }
}
