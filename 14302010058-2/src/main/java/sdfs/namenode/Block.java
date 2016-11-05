package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.datanode.DataNode;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * Created by shiyuhong on 16/10/25.
 */
public class Block {
    //应该封装Locatedblock?
    private final byte[] data = new byte[DataNode.BLOCK_SIZE];//data是DataNode.BLOCK_SIZE的，limit实际控制着放进来的数据大小
    private boolean isDirty = false;
    private int limit = 0;
    private int position = 0;
    //每次写回都只写回一个locatedBlock，所以只存一个位置的副本就好了
    private final InetAddress inetAddress;
    private final int blockNumber;


    public Block(byte[] data, InetAddress inetAddress, int blockNumber) {
        this.inetAddress = inetAddress;
        this.blockNumber = blockNumber;
        if (data.length <= DataNode.BLOCK_SIZE) {
            setdata(data);
            limit = data.length;
        } else {
            System.out.println("放入data的长度大于一个块的长度");
        }
    }

    public Block(InetAddress inetAddress, int blockNumber) {
        this.inetAddress = inetAddress;
        this.blockNumber = blockNumber;
        //空的data，大小为DataNode.BLOCK_SIZE
    }

    public void writeBack(UUID fileuuid) {
        DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
        try {
            byte[] newData=new byte[limit];
            System.arraycopy(data,0,newData,0,limit);
            dataNodeStub.write(fileuuid, blockNumber, 0, newData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public boolean isDirty() {
        return isDirty;
    }

    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    public int read(ByteBuffer dst) throws IOException {
        //将posiiton开始的字节读入dst，直到data读完或者dst满了为止
        int ret = 0;
        while (dst.hasRemaining() && position < limit) {
            ret++;
            dst.put(data[position]);
            position++;
        }
        return ret;
    }

    public int write(ByteBuffer src) {
        int ret = 0;
        while (src.hasRemaining() && position < data.length) {
            ret++;
            data[position] = src.get();
            position++;
            setDirty(true);
        }
        limit=position+1;
        if (position < data.length) {
            for (int i = position; i < data.length; i++) {
                data[i] = 0;
            }
        }
        return ret;
    }

    public void changeData(byte[] b, int offset) {
        for (int i = offset, j = 0; i < getData().length && j < b.length; i++, j++) {
            getData()[i] = b[j];
        }
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return Arrays.toString(data);
    }

    public void setdata(byte[] data) {
        if (data.length <= DataNode.BLOCK_SIZE) {
            for (int i = 0; i < data.length; i++) {
                this.data[i] = data[i];
            }
            limit = data.length;
        }
    }

    public void clearData() {
        for (byte b : data) {
            b = 0;
        }
    }

    public int getSize() {
        return limit;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        if(position<limit){
            this.position = position;
        }
    }
}
