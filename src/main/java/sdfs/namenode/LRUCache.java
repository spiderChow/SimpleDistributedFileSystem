package sdfs.namenode;

import sdfs.client.DataNodeStub;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by shiyuhong on 16/10/25.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int fileDataBlockCacheSize;
    private UUID fileuuid;
    //each block will be 64k

    public LRUCache(int cacheSize,UUID fileuuid) {
        super((int) Math.ceil(cacheSize / 0.75) + 1, 0.75f, true);
        fileDataBlockCacheSize = cacheSize;
        this.fileuuid=fileuuid;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        if (size() > fileDataBlockCacheSize) {
            //判断是否dirty,然后写回disk
            Block block = (Block) eldest.getValue();
            if (block.isDirty()) {
                //write back

                block.writeBack(fileuuid);
            }
            return true;
        } else {
            return false;
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, V> entry : entrySet()) {
            sb.append(String.format("%s:%s ", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    public void printState() {
        Iterator iterator = this.keySet().iterator();
        while (iterator.hasNext()) {
            System.out.println(this.get(iterator.next()));
        }
        System.out.println();

    }
}
