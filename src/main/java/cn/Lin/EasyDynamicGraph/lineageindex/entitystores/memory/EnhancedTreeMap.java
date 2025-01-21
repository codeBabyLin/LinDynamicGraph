package cn.Lin.EasyDynamicGraph.lineageindex.entitystores.memory;

import cn.Lin.EasyDynamicGraph.entities.InMemoryEntity;
import cn.Lin.EasyDynamicGraph.entities.InMemoryEntityFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class EnhancedTreeMap<K, V extends InMemoryEntity> {

    TreeMap<K, List<Pair<Long, V>>> tree;

    Comparator<Pair<Long, V>> comparator = Comparator.comparing(Pair::getLeft);

    public EnhancedTreeMap(Comparator<K> keyComp) {
        tree = new TreeMap<>(keyComp);
    }

    public void put(K key, V value) {
        tree.putIfAbsent(key, new ArrayList<>());
        List<Pair<Long,V>> entries = tree.get(key);

        // todo: create full entries if a threshold is reached
        /*if (!entries.isEmpty() && !value.isDeleted()) {
            value.setDiff();
        }*/
        insertInPlace(entries, new ImmutablePair<>(value.getStartTimestamp(), value));
    }

    public Optional<V> get(K key, long timestamp) {
        List<Pair<Long,V>> entries = tree.get(key);
        return constructEntityFromEntries(entries, timestamp);
    }

    public Optional<V> getFirstEntry(K key) {
        List<Pair<Long,V>> entries = tree.get(key);
        if (entries == null) {
            return Optional.empty();
        }

        return Optional.of(constructEntity(entries, 0));
    }

    public Optional<V> getLastEntry(K key) {
        List<Pair<Long,V>> entries = tree.get(key);
        if (entries == null) {
            return Optional.empty();
        }

        return Optional.of(constructEntity(entries, entries.size() - 1));
    }

    public boolean setDeleted(K key, long timestamp) {
        List<Pair<Long,V>> entries = tree.get(key);
        if (entries == null || entries.get(entries.size() - 1).getRight().getStartTimestamp() != timestamp) {
            return false;
        }
        entries.get(entries.size() - 1).getRight().setDeleted();
        return true;
    }

    public List<V> getAll(long timestamp) {
        ArrayList<V> result = new ArrayList<V>();
        for (K key : tree.keySet()) {
            Optional<V> element = get(key, timestamp);
            element.ifPresent(result::add);
        }
        return result;
    }

    public List<V> rangeScanByKey(K fromKey, K toKey, long timestamp) {
        ArrayList<V> result = new ArrayList<V>();
        SortedMap<K,List<Pair<Long,V>>> subMap = tree.subMap(fromKey, toKey);
        for (Map.Entry<K,List<Pair<Long,V>>> e : subMap.entrySet()) {
            Optional<V> element = constructEntityFromEntries(e.getValue(), timestamp);
            element.ifPresent(result::add);
        }
        return result;
    }

    public List<V> rangeScanByTime(K key, long fromTimestamp, long toTimestamp) {
        List<Pair<Long,V>> entries = tree.get(key);

        ArrayList<V> result = new ArrayList<V>();
        int currentPos = Math.max(searchForTime(entries, fromTimestamp), 0);
        Optional<V> currentNode =
                Optional.of((V) InMemoryEntityFactory.newEntity(entries.get(0).getRight()));
        for (; currentPos < entries.size(); currentPos++) {
            Pair<Long,V> entry = entries.get(currentPos);
            if (entry.getRight().getStartTimestamp() > toTimestamp) {
                break;
            }

            currentNode.get().merge(entry.getRight());
            updateLastElement(result, currentNode.get().getStartTimestamp());
            if (!currentNode.get().isDeleted()) {
                result.add(currentNode.get());
                currentNode = Optional.of((V) currentNode.get().copy());
            } else {
                currentNode = Optional.of(
                        (V) InMemoryEntityFactory.newEntity(entries.get(0).getRight()));
            }
        }
        return result;
    }

    public List<List<V>> rangeScanByKeyAndTime(K fromKey, K toKey, long fromTimestamp, long toTimestamp) {
        ArrayList<List<V>> result = new ArrayList<List<V>>();
        SortedMap<K,List<Pair<Long,V>>> subMap = tree.subMap(fromKey, toKey);
        for (Map.Entry<K,List<Pair<Long,V>>> e : subMap.entrySet()) {
            ArrayList<V> subResult = new ArrayList<V>();
            for (Pair<Long, V> update : e.getValue()) {
                long updateTimestamp = update.getRight().getStartTimestamp();
                if (updateTimestamp >= fromTimestamp && updateTimestamp <= toTimestamp) {
                    // todo: optimize the result construction by reusing previous results
                    Optional<V> element = constructEntityFromEntries(e.getValue(), updateTimestamp);
                    element.ifPresent(subResult::add);
                }
            }
            result.add(subResult);
        }
        return result;
    }

    private void insertInPlace(List<Pair<Long, V>> values, Pair<Long, V> value) {
        int pos = Collections.binarySearch(values, value, comparator);
        if (pos < 0) {
            values.add(-pos - 1, value);
        } else {
            values.add(pos + 1, value);
        }
    }

    private Optional<V> constructEntityFromEntries(List<Pair<Long, V>> entries, long timestamp) {
        if (entries == null) {
            return Optional.empty();
        }

        int pos = searchForTime(entries, timestamp);

        if (pos < 0 || entries.get(pos).getRight().isDeleted()) {
            return Optional.empty();
        }
        // Reconstruct entity
        return Optional.of(constructEntity(entries, pos));
    }

    private int searchForTime(List<Pair<Long, V>> entries, long timestamp) {
        // search for the first entry that has a timestamp larger or equal than the argument provided
        int pos = Collections.binarySearch(entries, new ImmutablePair<>(timestamp, null), comparator);
        pos = (pos < 0) ? -pos - 2 : pos;
        return pos;
    }

    private V constructEntity(List<Pair<Long, V>> entries, int pos) {
        // If this is the first element of the chain, or this is a complete record (i.e., not a diff),
        // or the previous entity was deleted we found the result
        if (pos == 0
                || !entries.get(pos).getRight().isDiff()
                || entries.get(pos - 1).getRight().isDeleted()) {
            return entries.get(pos).getRight();
        }

        // Otherwise, we need to reconstruct it. First, we need to find from which point to start.
        int firstPos = pos;
        while (firstPos >= 0) {
            if (entries.get(pos).getRight().isDeleted()) {
                break;
            }

            firstPos--;

            if (firstPos == 0
                    || (!entries.get(pos).getRight().isDiff()
                    || entries.get(pos).getRight().isDeleted())) {
                break;
            }
        }

        // Construct final result
        InMemoryEntity result = InMemoryEntityFactory.newEntity(entries.get(0).getRight());
        for (int i = firstPos; i <= pos; ++i) {
            result.merge(entries.get(i).getRight());
        }

        return (V) result;
    }

    public void reset() {
        tree.clear();
    }

    private void updateLastElement(List<V> result, long timestamp) {
        if (!result.isEmpty()) {
            result.get(result.size() - 1).setEndTimestamp(timestamp);
        }
    }
}
