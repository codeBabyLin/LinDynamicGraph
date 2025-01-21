//package cn.Lin.EasyDynamicGraph.graphdb.persistent.cache;
//
//import cn.Lin.EasyDynamicGraph.graphdb.kernel.BasicGraph;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class CLOCKCachePlus implements CacheInterface{
//    private HashMap<Integer, BasicGraph<Long, Long>> graphPool;
//    private int cacheSze;
//
//    private int initScore;
//    private HashMap<Integer, Integer> clock;
//    public CLOCKCachePlus(int cacheSize){
//        this.cacheSze = cacheSize;
//        //this.queue = new ArrayDeque<>();
//        this.clock = new HashMap<>();
//        this.initScore = 2;
//    }
//
//    private int findMinValueKey(HashMap<Integer, Integer> clock){
//        int flagIndex = -1;
//        int MinValue = Integer.MAX_VALUE;
//        for(Map.Entry<Integer, Integer> pair: clock.entrySet()){
//            int key = pair.getKey();
//            int value = pair.getValue();
//            if(value<=MinValue){
//                flagIndex = key;
//                MinValue = value;
//            }
//        }
//        return flagIndex;
//    }
//    private void setClockTime(){
//        for(Integer e: clock.keySet()){
//            int value = this.clock.get(e);
//            this.clock.put(e,value-1);
//
//        }
//    }
//    private void setRelativeVersionTime(int version){
//        for(Integer e: clock.keySet()){
//            if(e-version<=2) {
//                int value = this.clock.get(e);
//                this.clock.put(e, value + 1);
//            }
//        }
//    }
//    @Override
//    public void cacheGraph(BasicGraph<Long, Long> graph, int version) {
//        if(graphPool.size()>=this.cacheSze){
//            int v = findMinValueKey(this.clock);
//            this.clock.remove(v);
//            this.graphPool.remove(v);
//            setClockTime();
//
//        }
//        this.graphPool.put(version,graph);
//        this.clock.put(version,initScore);
//        setRelativeVersionTime(version);
//    }
//
//    @Override
//    public boolean exist(int version) {
//        return  this.graphPool.containsKey(version);
//    }
//
//    @Override
//    public BasicGraph<Long, Long> hitGraph(int version) {
//        BasicGraph<Long, Long> graph = this.graphPool.get(version);
//        cacheGraph(graph,version);
//        return graph;
//    }
//}
