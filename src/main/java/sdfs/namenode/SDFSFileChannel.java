/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.datanode.DataNode;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
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


    SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly, int fileDataBlockCacheSize) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;
        cache = new LRUCache<>(fileDataBlockCacheSize);//(the hashcode of locatedblock, block)
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
        return ret;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int ret = 0;
        fileSize=src.remaining();
        while (src.hasRemaining()) {
            Block block = blockAtPosition((int) position);
            int byteNum = block.write(src);
            ret += byteNum;
            position += byteNum;
        }
        return ret;
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
        if (size < fileSize) {
            fileSize = (int) size;
            //discard
            //????
        }
        return null;
    }

    @Override
    public boolean isOpen() {
        //todo your code here
        return false;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void flush() throws IOException {
        //todo your code here
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
                DataNodeStub dataNodeStub = new DataNodeStub();//??连接
                LocatedBlock locatedBlock = chooseOneInBlockInfo(blockInfo);
                try {
                    byte[] data = dataNodeStub.read(uuid, locatedBlock.getBlockNumber(), 0, DataNode.BLOCK_SIZE);
                    block = new Block(data, ,blockNumber);
                    cache.put(locatedBlock.hashCode(), block);
                    return block;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            //需要新添加blockInfo
            NameNode nameNode = new NameNode(NameNode.NAME_NODE_PORT);
            LocatedBlock locatedBlock = nameNode.addBlock(uuid);
            //放进cache
            Block block = new Block(inetAddress);
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


}
