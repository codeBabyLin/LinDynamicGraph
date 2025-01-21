//package cn.Lin.EasyDynamicGraph.graphdb.persistent.cache;
//
//import cn.Lin.EasyDynamicGraph.graphdb.kernel.BasicGraph;
//
//import java.util.ArrayDeque;
//import java.util.HashMap;
//
//public class FIFOCache implements CacheInterface{
//
//
//    private HashMap<Integer,BasicGraph<Long, Long>> graphPool;
//    private int cacheSze;
//
//    private ArrayDeque<Integer> queue;
//    public FIFOCache(int cacheSize){
//        this.cacheSze = cacheSize;
//        this.queue = new ArrayDeque<>();
//    }
//
//    @Override
//    public void cacheGraph(BasicGraph<Long, Long> graph, int version) {
//        if(graphPool.size()>=this.cacheSze){
//            int v = this.queue.pollFirst();
//            this.graphPool.remove(v);
//
//        }
//        this.graphPool.put(version,graph);
//        this.queue.addLast(version);
//    }
//
//    @Override
//    public boolean exist(int version) {
//        return  this.queue.contains(version);
//    }
//
//    @Override
//    public BasicGraph<Long, Long> hitGraph(int version) {
//        BasicGraph<Long, Long> graph = this.graphPool.get(version);
//        cacheGraph(graph,version);
//        return this.graphPool.get(version);
//    }
//}
