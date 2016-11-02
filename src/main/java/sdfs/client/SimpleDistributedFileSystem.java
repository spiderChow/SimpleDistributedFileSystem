/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.namenode.NameNode;
import sdfs.namenode.SDFSFileChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {


    // InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", NameNode.NAME_NODE_PORT);
    //SimpleDistributedFileSystem simpleDistributedFileSystem = new SimpleDistributedFileSystem(inetSocketAddress, 0);


    public static void main(String args[]) throws Exception {
        // NameNodeStub nameNodeStub = new NameNodeStub();
        //  nameNodeStub.mkdir("dir");
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", NameNode.NAME_NODE_PORT);
        SimpleDistributedFileSystem simpleDistributedFileSystem = new SimpleDistributedFileSystem(inetSocketAddress, 1);
        //mkdir
        simpleDistributedFileSystem.mkdir("Dir");
        System.out.println("mkdir 完成");
        //create
        SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.create("b.txt");
        sdfsFileChannel.close();
        System.out.println("create 完成");

        //write
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byte[] bytes = {'a','b','e'};
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        sdfsFileChannel.write(byteBuffer);
        sdfsFileChannel.close();
        System.out.println("write 完成");
        //read
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byteBuffer =ByteBuffer.allocate(3);
        sdfsFileChannel.read(byteBuffer);
        System.out.println("read 完成");
        //打印buffer
        System.out.println(byteBuffer.toString());
        byte[] content=byteBuffer.array();
        System.out.println(Arrays.toString(content));
        sdfsFileChannel.close();

        /*
        //write 2 block
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        ByteBuffer byteBuffer2 =ByteBuffer.allocate(1024*64+1);
        int i=0;
        while(byteBuffer2.hasRemaining()){
            byteBuffer.putInt(i);
            i++;
        }
        sdfsFileChannel.write(byteBuffer);
        sdfsFileChannel.close();
        //read
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byteBuffer2 =ByteBuffer.allocate(1024*64+1);
        sdfsFileChannel.read(byteBuffer2);
        //打印buffer
        System.out.println(byteBuffer2.toString());
        sdfsFileChannel.close();
        */


    }

    private NameNodeStub nameNodeStub;
    private int fileDataBlockCacheSize;

    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     *                               That means 16 block of data will be cache on local.
     *                               And you should use LRU algorithm to replace it.
     *                               It may be change during test. So don't assert it will equal to a constant.
     */
    public SimpleDistributedFileSystem(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        //todo your code here
        nameNodeStub = new NameNodeStub(nameNodeAddress);
        this.fileDataBlockCacheSize=fileDataBlockCacheSize;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadonly(fileUri);
        sdfsFileChannel.setFileDataBlockCacheSize(fileDataBlockCacheSize);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.create(fileUri);
        sdfsFileChannel.setFileDataBlockCacheSize(fileDataBlockCacheSize);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadwrite(fileUri);
        sdfsFileChannel.setFileDataBlockCacheSize(fileDataBlockCacheSize);
        return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        nameNodeStub.mkdir(fileUri);
    }
}
