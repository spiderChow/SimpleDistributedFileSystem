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
/*
        //mkdir
        simpleDistributedFileSystem.mkdir("Dir");
        System.out.println("mkdir 完成");
        */
        //create
        SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("Dir/a.txt");
        byte[] bytes = {'z','x','y','q','a'};
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        sdfsFileChannel.position(7);
        sdfsFileChannel.write(byteBuffer);
        byteBuffer =ByteBuffer.allocate(7);
        sdfsFileChannel.position(0);
        sdfsFileChannel.read(byteBuffer);
        //打印buffer
        System.out.println(byteBuffer.toString());
        byte[] content=byteBuffer.array();
        System.out.println(Arrays.toString(content));

        sdfsFileChannel.close();
        System.out.println("create 完成");
        /*
        //write
        SDFSFileChannel sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byte[] bytes = {'a','c','e','q','3'};
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        sdfsFileChannel.write(byteBuffer);

        byteBuffer =ByteBuffer.allocate(3);
        sdfsFileChannel.position(0);
        sdfsFileChannel.read(byteBuffer);
        //打印buffer
        System.out.println(byteBuffer.toString());
        byte[] content=byteBuffer.array();
        System.out.println(Arrays.toString(content));
        sdfsFileChannel.close();
        //System.out.println("write 完成");
        /*
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


        //write 2 block
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byte[] content2=new byte[1024*64+1];
        int i=0;
        while(i<1024*64+1){
            content2[i]= 'a';
            i++;
        }
        ByteBuffer byteBuffer2 =ByteBuffer.wrap(content2);
        sdfsFileChannel.write(byteBuffer2);
        sdfsFileChannel.close();
        System.out.println("write2 完成");
        //read
        sdfsFileChannel = simpleDistributedFileSystem.openReadWrite("b.txt");
        byteBuffer2 =ByteBuffer.allocate(1024*64+1);
        sdfsFileChannel.read(byteBuffer2);
        //打印buffer
        System.out.println(byteBuffer2.toString());

        byte[] content3=byteBuffer2.array();
        System.out.println(Arrays.toString(content3));
        sdfsFileChannel.close();
        System.out.println("read2 完成");

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
