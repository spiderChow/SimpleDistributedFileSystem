/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.message.Message;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
    String host = "127.0.0.1";  //要连接的服务端IP地址
    int port = DataNode.DATA_NODE_PORT;   //要连接的服务端对应的监听端口


    //ip地址和port都已经写死了

    public DataNodeStub(InetAddress inetAddress) {
        this.host = inetAddress.getHostName();
        //this.port = inetAddress.getPort();
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        //read data from ip: LocatedBlock.inetAddress ;blockNum: LocatedBlcok.blockNumber
        byte[] ret = new byte[0];

        //与服务端建立连接
        Socket client = null;
        try {
            client = new Socket(host, port);

            //建立连接后就可以往服务端写数据了
            Message message = new Message();//message????
            message.setMethodName("read");
            Class[] ParameterTypes = new Class[]{UUID.class, int.class, int.class, int.class};
            //UUID fileUuid, int blockNumber, int offset, int size
            message.setParameterTypes(ParameterTypes);
            Object[] Parametes = new Object[]{fileUuid, blockNumber, offset, size};
            message.setParams(Parametes);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(client.getOutputStream());
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            objectOutputStream.close();

            //从服务器读取响应
            ObjectInputStream objectInputStream = new ObjectInputStream(client.getInputStream());
            try {
                message = (Message) objectInputStream.readObject();
                ret = (byte[]) message.getResult();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        //UUID fileUuid, int blockNumber, int offset, byte[] b
        byte[] ret = new byte[0];
        String host = "127.0.0.1";  //要连接的服务端IP地址
        int port = DataNode.DATA_NODE_PORT;   //要连接的服务端对应的监听端口
        //与服务端建立连接
        Socket client = null;
        try {
            client = new Socket(host, port);

            //建立连接后就可以往服务端写数据了
            Message message = new Message();//message????
            message.setMethodName("write");
            Class[] ParameterTypes = new Class[]{UUID.class, int.class, int.class, byte[].class};
            //UUID fileUuid, int blockNumber, int offset, int size
            message.setParameterTypes(ParameterTypes);
            Object[] Parametes = new Object[]{fileUuid, blockNumber, offset, b};
            message.setParams(Parametes);

            ObjectOutputStream objectOutputStream = new ObjectOutputStream(client.getOutputStream());
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            // objectOutputStream.close();
            client.shutdownOutput();

            //从服务器读取响应
            ObjectInputStream objectInputStream = new ObjectInputStream(client.getInputStream());
            try {
                message = (Message) objectInputStream.readObject();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
