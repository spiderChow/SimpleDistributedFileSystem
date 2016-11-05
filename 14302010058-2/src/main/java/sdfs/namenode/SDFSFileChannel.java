/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.client.NameNodeStub;
import sdfs.datanode.DataNode;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private final UUID uuid; //File uuid
    private int fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    private final FileNode fileNode;
    private long position = 0;
    private final boolean isReadOnly;
    //private final Map<Integer, byte[]> dataBlocksCache = new HashMap<>(); //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.
    private LRUCache<Integer, Block> cache = null;//(the hashcode of locatedblock, block)


    SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;
        // cache = new LRUCache<>(fileDataBlockCacheSize,uuid);//(the hashcode of locatedblock, block)
    }

    public void setFileDataBlockCacheSize(int fileDataBlockCacheSize) {
        cache = new LRUCache<>(fileDataBlockCacheSize, uuid);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        //read的字节数就是将dst填满或者读完所有的file的字节
        int ret = 0;
        while (dst.hasRemaining() && position < fileSize) {
            Block block = blockAtPosition((int) position);
            int byteNum = block.read(dst);
            ret += byteNum;
            position += byteNum;
        }
        //printCache();
        return ret;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isReadOnly) {
            int ret = 0;
            truncate((int) (src.remaining() + position));
            while (src.hasRemaining()) {
                Block block = blockAtPosition((int) position);
                block.setDirty(true);
                int byteNum = block.write(src);
                ret += byteNum;
                position += byteNum;
            }
            //printCache();
            return ret;
        }
        return -1;
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        return fileSize;
    }

    @Override
    /*
    * If the given size is less than the current size then the entity is truncated, discarding any bytes beyond the new end.
    * If the given size is greater than or equal to the current size then the entity is not modified.
    * In either case, if the current position is greater than the given size then it is set to that size
    * */
    public SeekableByteChannel truncate(long size) throws IOException {
        //计算是否要移除最后的LocatedBlock
        if (size < fileSize) {
            fileSize = (int) size;
            int newBlockNum = fileSize / DataNode.BLOCK_SIZE;
            if (fileSize > newBlockNum * DataNode.BLOCK_SIZE) {
                newBlockNum++;
            }
            //所有block内容全部清为0
            //dataNode删掉block内容
            InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", NameNode.NAME_NODE_PORT);
            NameNodeStub nameNodeStub = new NameNodeStub(inetSocketAddress);
            nameNodeStub.removeLastBlocks(uuid, blockAmount - newBlockNum);
            for (int i = 0; i < blockAmount - newBlockNum; i++) {
                fileNode.removeLastBlockInfo();
            }
            return this;
        }
       fileSize = (int) size;
        return null;
    }

    @Override
    public boolean isOpen() {
        if (cache != null) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        //由于是为每一次请求保留cache，所以close时候要将所有的cahce写回
        Iterator<Map.Entry<Integer, Block>> iterator = cache.entrySet().iterator();
        if (iterator.hasNext()) {
            Block block = iterator.next().getValue();
            if (block.isDirty()) {
                block.writeBack(uuid);
            }
        }
        cache.clear();
        cache = null;
        //改变fileNode内容并保存（只需要将fileSize保存下来就可以了）
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", NameNode.NAME_NODE_PORT);
        NameNodeStub nameNodeStub = new NameNodeStub(inetSocketAddress);
        if (isReadOnly) {
            nameNodeStub.closeReadonlyFile(uuid);
        } else {
            nameNodeStub.closeReadwriteFile(uuid, fileSize);
        }
    }

    @Override
    public void flush() throws IOException {
    }

    private Block blockAtPosition(int i) {
        if (i < fileSize && i >= 0) {
            //之前有i个byte
            int a = i / DataNode.BLOCK_SIZE;
            //从第a+1个block，即index为a的block开始读
            Block block = getCacheBlockOfBlockInfo(a);
            int p = i - a * DataNode.BLOCK_SIZE;
            block.setPosition(p);
            return block;
        }
        return null;
    }

    /*
    * 输入的i为fileNode的index为i的blockInfo
    * 若i< blockAmount,则已分配LocatedBlock -->在cache中则返回block,不在就读进cache,返回block
    * 若i> blockAmount,则未分配LocatedBlock -->要求nameNodeStub返回LocatedBlock,直接放于cache,data置为0;
    *
    * */
    private Block getCacheBlockOfBlockInfo(int i) {
        if (i < fileNode.getBlockAmount()) {//已经分配过blockInfo了
            BlockInfo blockInfo = fileNode.getBlockInfoAt(i);
            Block block = isIncache(blockInfo);
            if (block != null) {
                //在cache
                return block;
            } else {
                //不在cache
                LocatedBlock locatedBlock = chooseOneInBlockInfo(blockInfo);
                DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getInetAddress());//??连接
                try {
                    byte[] data = dataNodeStub.read(uuid, locatedBlock.getBlockNumber(), 0, DataNode.BLOCK_SIZE);
             //       System.out.println("channel getCacheBlockOfBlockInfo:" + Arrays.toString(data));
                    block = new Block(data, locatedBlock.getInetAddress(), locatedBlock.getBlockNumber());
                    cache.put(locatedBlock.hashCode(), block);
                    return block;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            //需要新添加blockInfo，添加一次，NameNode就将fileNode里面的BlockInfo保存在disk上了
            InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", NameNode.NAME_NODE_PORT);
            NameNodeStub nameNodeStub = new NameNodeStub(inetSocketAddress);
            LocatedBlock locatedBlock = nameNodeStub.addBlock(uuid);
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(locatedBlock);
            fileNode.addBlockInfo(blockInfo);
            blockAmount++;
            //放进cache
            Block block = new Block(locatedBlock.getInetAddress(), locatedBlock.getBlockNumber());
            cache.put(locatedBlock.hashCode(), block);
            return block;
        }
        return null;
    }


    private Block isIncache(BlockInfo blockInfo) {
        //BlockInfo has many LocatedBlocks
        Iterator<LocatedBlock> locatedBlockIterator = blockInfo.iterator();
        while (locatedBlockIterator.hasNext()) {
            LocatedBlock locatedBlock = locatedBlockIterator.next();
            if (cache.containsKey(locatedBlock.hashCode())) {
                //在cache
                return cache.get(locatedBlock.hashCode());
            }
        }
        return null;
    }


    private LocatedBlock chooseOneInBlockInfo(BlockInfo blockInfo) {
        //返回第一个LocatedBlock
        Iterator<LocatedBlock> locatedBlockIterator = blockInfo.iterator();
        LocatedBlock locatedBlock = locatedBlockIterator.next();
        return locatedBlock;
    }

    public void printCache() {
        //System.out.println("打印cache状态：" + cache.size());
        // System.out.println();
        Iterator<Map.Entry<Integer, Block>> iterator = cache.entrySet().iterator();
        if (iterator.hasNext()) {
            Block block = iterator.next().getValue();
            byte[] data = block.getData();
          //  System.out.println(Arrays.toString(data));
        }
    }
}
