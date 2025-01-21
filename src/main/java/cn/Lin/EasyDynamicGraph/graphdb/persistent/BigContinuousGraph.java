//package cn.Lin.EasyDynamicGraph.graphdb.persistent;
//
//import cn.Lin.EasyDynamicGraph.graphdb.kernel.BasicGraph;
//import cn.Lin.EasyDynamicGraph.graphdb.persistent.cache.CacheInterface;
//
//import java.io.File;
//import java.util.Iterator;
//
//public class BigContinuousGraph{
//
//
//
//    private IdVersionStore nodeIdCStore;
//    private IdVersionStore nodeIdDStore;
//    private IdVersionStore relationIdCStore;
//    private IdVersionStore relationIdDStore;
//    private Transformer transformer;
//    private CacheInterface cache;
//    public BigContinuousGraph(String dir){
//        this.nodeIdCStore = new IdVersionStore(new File(dir,"nodeIdCreate").getAbsolutePath());
//        this.nodeIdDStore = new IdVersionStore(new File(dir,"nodeIdDelete").getAbsolutePath());
//        this.relationIdCStore = new IdVersionStore(new File(dir,"relationIdCreate").getAbsolutePath());
//        this.relationIdDStore = new IdVersionStore(new File(dir,"relationIdDelete").getAbsolutePath());
//        this.transformer = new Transformer();
//    }
//    public BigContinuousGraph(String dir, CacheInterface cache){
//        this.nodeIdCStore = new IdVersionStore(new File(dir,"nodeIdCreate").getAbsolutePath());
//        this.nodeIdDStore = new IdVersionStore(new File(dir,"nodeIdDelete").getAbsolutePath());
//        this.relationIdCStore = new IdVersionStore(new File(dir,"relationIdCreate").getAbsolutePath());
//        this.relationIdDStore = new IdVersionStore(new File(dir,"relationIdDelete").getAbsolutePath());
//        this.transformer = new Transformer();
//        this.cache = cache;
//    }
//
//    public boolean existNode(long node){
//        return this.nodeIdCStore.exist(transformer.LongToByte(node));
//    }
//    public boolean existRelation(long relation){
//        return this.relationIdCStore.exist(transformer.LongToByte(relation));
//    }
//    public void addNode(long node,long version){
//
//        byte[]key = transformer.LongToByte(node);
//        byte[]cValue = transformer.LongToByte(version);
//        byte[]dValue = transformer.LongToByte(Long.MAX_VALUE);
//        this.nodeIdCStore.add(key,cValue);
//        this.nodeIdDStore.add(key,dValue);
//
//    }
//    public void deleteNode(long node,long version){
//        byte[]key = transformer.LongToByte(node);
//        byte[]dValue = transformer.LongToByte(version);
//        this.nodeIdDStore.add(key,dValue);
//    }
//    public void deleteNode(byte[]key,long version){
//        //byte[]key = transformer.LongToByte(node);
//        byte[]dValue = transformer.LongToByte(version);
//        this.nodeIdDStore.add(key,dValue);
//    }
//    public void addRelation(long relation,long version){
//        byte[]key = transformer.LongToByte(relation);
//        byte[]cValue = transformer.LongToByte(version);
//        byte[]dValue = transformer.LongToByte(Long.MAX_VALUE);
//        this.relationIdCStore.add(key,cValue);
//        this.relationIdDStore.add(key,dValue);
//    }
//    public void deleteRelation(long relation,long version){
//        byte[]key = transformer.LongToByte(relation);
//        byte[]dValue = transformer.LongToByte(version);
//        this.relationIdDStore.add(key,dValue);
//    }
//    public void deleteRelation( byte[]key ,long version){
//        //byte[]key = transformer.LongToByte(relation);
//        byte[]dValue = transformer.LongToByte(version);
//        this.relationIdDStore.add(key,dValue);
//    }
//
//    public Iterator<Long> AllNodes(){
//        Iterator<byte[]> iter = this.nodeIdCStore.all();
//        return new Iterator<Long>() {
//            @Override
//            public boolean hasNext() {
//                return iter.hasNext();
//            }
//
//            @Override
//            public Long next() {
//                byte[]data = iter.next();
//                long e = transformer.ByteToLong(data);
//                return e;
//            }
//        };
//    }
//    public Iterator<Long> AllRelations(){
//        Iterator<byte[]> iter = this.relationIdCStore.all();
//        return new Iterator<Long>() {
//            @Override
//            public boolean hasNext() {
//                return iter.hasNext();
//            }
//
//            @Override
//            public Long next() {
//                byte[]data = iter.next();
//                long e = transformer.ByteToLong(data);
//                return e;
//            }
//        };
//    }
//    public long getNodeCreateVersion(long node){
//        byte[]key = transformer.LongToByte(node);
//        byte[]value = this.nodeIdCStore.get(key);
//        long v = transformer.ByteToLong(value);
//        return v;
//    }
//    public long getNodeDeleteVersion(long node){
//        byte[]key = transformer.LongToByte(node);
//        byte[]value = this.nodeIdDStore.get(key);
//        long v = transformer.ByteToLong(value);
//        return v;
//    }
//    public long getRelationCreateVersion(long relation){
//        byte[]key = transformer.LongToByte(relation);
//        byte[]value = this.relationIdCStore.get(key);
//        long v = transformer.ByteToLong(value);
//        return v;
//    }
//    public long getRelationDeleteVersion(long relation){
//        byte[]key = transformer.LongToByte(relation);
//        byte[]value = this.relationIdDStore.get(key);
//        long v = transformer.ByteToLong(value);
//        return v;
//    }
//
//    public void setNodeCreateVersion(byte[]key, byte[]value){
//        this.nodeIdCStore.add(key,value);
//    }
//    public void setNodeDeleteVersion(byte[]key, byte[]value){
//        this.nodeIdDStore.add(key,value);
//    }
//    public void setRelationCreateVersion(byte[]key, byte[]value){
//        this.relationIdCStore.add(key,value);
//    }
//    public void setRelationDeleteVersion(byte[]key, byte[]value){
//        this.relationIdDStore.add(key,value);
//    }
//
//    private boolean isOk(long v, long s, long e){
//        return s <= v && (v <= e);
//    }
//    private boolean isOk(long vs,long ve,long ds,long de){
//        return ds<=vs && (ve<=de);
//    }
//
//    public Iterator<Long> AllNodesByVersionCache(Long version){
//        long v = version;
//        int v1 = (int) v;
//        if(this.cache.exist(v1)){
//            return this.cache.hitGraph(v1).AllNodes();
//        }
//        else{
//            BasicGraph<Long, Long> graph = new BasicGraph<>();
//            Iterator<Long> iter = AllNodesByVersion(version);
//            while(iter.hasNext()){
//                graph.addNode(iter.next());
//            }
//            return this.cache.hitGraph(v1).AllNodes();
//        }
//
//    }
//
//    public Iterator<Long> AllNodesByVersion(Long version){
//        Iterator<byte[]> iter = this.nodeIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//            @Override
//            public boolean hasNext() {
//                if(iter.hasNext()){
//                    boolean isCan = false;
//                    while(!isCan&& iter.hasNext()){
//                        byte[]key = iter.next();
//                        byte[]cValue = nodeIdCStore.get(key);
//                        byte[]dValue = nodeIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if(isOk(version,vs,ve)){
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//
//                    return isCan;
//
//                }
//                else{
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//    }
//    public Iterator<Long> AllRelationsByVersion(Long version){
//        Iterator<byte[]> iter = this.relationIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//            @Override
//            public boolean hasNext() {
//                if(iter.hasNext()){
//                    boolean isCan = false;
//                    while(!isCan&& iter.hasNext()) {
//                        byte[] key = iter.next();
//                        byte[] cValue = relationIdCStore.get(key);
//                        byte[] dValue = relationIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if (isOk(version, vs, ve)) {
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//                    return isCan;
//                }
//                else{
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//    }
//
//    public Iterator<Long> AllNodesByVersion(Long vStart, Long vEnd ){
//        Iterator<byte[]> iter = this.nodeIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//            @Override
//            public boolean hasNext() {
//                if(iter.hasNext()){
//                    boolean isCan = false;
//                    while(!isCan&& iter.hasNext()){
//                        byte[]key = iter.next();
//                        byte[]cValue = nodeIdCStore.get(key);
//                        byte[]dValue = nodeIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if(isOk(vStart,vEnd,vs,ve)){
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//
//                    return isCan;
//
//                }
//                else{
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//    }
//    public Iterator<Long> AllRelationsByVersion(Long vStart, Long vEnd) {
//        Iterator<byte[]> iter = this.relationIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//
//            @Override
//            public boolean hasNext() {
//                if (iter.hasNext()) {
//                    boolean isCan = false;
//                    while (!isCan && iter.hasNext()) {
//                        byte[] key = iter.next();
//                        byte[] cValue = relationIdCStore.get(key);
//                        byte[] dValue = relationIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if (isOk(vStart, vEnd, vs, ve)) {
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//                    return isCan;
//                } else {
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//
//    }
//    public void flush(){
//        this.nodeIdCStore.flush();
//        this.nodeIdDStore.flush();
//        this.relationIdCStore.flush();
//        this.relationIdDStore.flush();
//    }
//
//    public BigContinuousGraph subGraph(long vStart, long vEnd){
//        return subGraph(vStart,vEnd, String.format("./%d_%d",vStart,vEnd));
//    }
//    public BigContinuousGraph subGraph(long vStart, long vEnd, String dir){
//
//        BigContinuousGraph suGraph = new BigContinuousGraph(dir);
//
//        Iterator<byte[]> iter = this.nodeIdCStore.all();
//        while(iter.hasNext()){
//            byte[]key = iter.next();
//            byte[] cValue = this.nodeIdCStore.get(key);
//            byte[] dValue = this.nodeIdDStore.get(key);
//            //long e = transformer.ByteToLong(key);
//            long vs = transformer.ByteToLong(cValue);
//            long ve = transformer.ByteToLong(dValue);
//            if(vs<=vStart && (vEnd<=ve)){
//                suGraph.setNodeCreateVersion(key,cValue);
//                suGraph.setNodeDeleteVersion(key,dValue);
//            }
//        }
//        Iterator<byte[]> iterator = this.relationIdCStore.all();
//        while(iterator.hasNext()){
//            byte[]key = iterator.next();
//            byte[] cValue = this.relationIdCStore.get(key);
//            byte[] dValue = this.relationIdDStore.get(key);
//            //long e = transformer.ByteToLong(key);
//            long vs = transformer.ByteToLong(cValue);
//            long ve = transformer.ByteToLong(dValue);
//            if(vs<=vStart && (vEnd<=ve)){
//                suGraph.setRelationCreateVersion(key,cValue);
//                suGraph.setRelationDeleteVersion(key,dValue);
//            }
//        }
//        return suGraph;
//    }
//
//
//    private boolean isOk(Long[] v, long s, long e){
//        boolean flag = false;
//        for (long l : v) {
//            if (isOk(l, s, e)) {
//                flag = true;
//                break;
//            }
//        }
//        return flag;
//    }
//    public Iterator<Long> AllNodesByMultiVersion(Long[]version){
//        Iterator<byte[]> iter = this.nodeIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//            @Override
//            public boolean hasNext() {
//                if(iter.hasNext()){
//                    boolean isCan = false;
//                    while(!isCan&& iter.hasNext()){
//                        byte[]key = iter.next();
//                        byte[]cValue = nodeIdCStore.get(key);
//                        byte[]dValue = nodeIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if(isOk(version,vs,ve)){
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//
//                    return isCan;
//
//                }
//                else{
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//    }
//    public Iterator<Long> AllRelationsByMultiVersion(Long version){
//        Iterator<byte[]> iter = this.relationIdCStore.all();
//        return new Iterator<Long>() {
//            long temp;
//            @Override
//            public boolean hasNext() {
//                if(iter.hasNext()){
//                    boolean isCan = false;
//                    while(!isCan&& iter.hasNext()) {
//                        byte[] key = iter.next();
//                        byte[] cValue = relationIdCStore.get(key);
//                        byte[] dValue = relationIdDStore.get(key);
//                        long e = transformer.ByteToLong(key);
//                        long vs = transformer.ByteToLong(cValue);
//                        long ve = transformer.ByteToLong(dValue);
//                        if (isOk(version, vs, ve)) {
//                            temp = e;
//                            isCan = true;
//                        }
//                    }
//                    return isCan;
//                }
//                else{
//                    return false;
//                }
//            }
//
//            @Override
//            public Long next() {
//                return temp;
//            }
//        };
//    }
//
//
//}
