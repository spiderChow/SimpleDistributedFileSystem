/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.message.Message;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {
    InetSocketAddress nameNodeAddress;

    public NameNodeStub(InetSocketAddress nameNodeAddress) {
        this.nameNodeAddress = nameNodeAddress;
    }

    public Message communicate(Message message) {
        SDFSFileChannel ret = null;
        // String host = "127.0.0.1";  //要连接的服务端IP地址
        // int port = NameNode.NAME_NODE_PORT;   //要连接的服务端对应的监听端口
        //与服务端建立连接
        Socket client = null;
        try {
            client = new Socket(nameNodeAddress.getHostName(), nameNodeAddress.getPort());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(client.getOutputStream());
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            // objectOutputStream.close();

            //从服务器读取响应
            ObjectInputStream objectInputStream = new ObjectInputStream(client.getInputStream());
            try {
                message = (Message) objectInputStream.readObject();
                //ret = (SDFSFileChannel) message.getResult();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return message;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        //建立连接后就可以往服务端写数据了
        Message message = new Message();
        message.setMethodName("openReadonly");
        Class[] ParameterTypes = new Class[]{String.class};
        //UUID fileUuid, int blockNumber, int offset, int size
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUri};
        message.setParams(Parametes);

        message = communicate(message);
        return (SDFSFileChannel) message.getResult();
        //判断回来的东西是否成功
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        Message message = new Message();

        message.setMethodName("openReadwrite");
        Class[] ParameterTypes = new Class[]{String.class};
        //UUID fileUuid, int blockNumber, int offset, int size
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUri};
        message.setParams(Parametes);
        message = communicate(message);

        return (SDFSFileChannel) message.getResult();

    }

    @Override
    public SDFSFileChannel create(String fileUri) {
        Message message = new Message();

        message.setMethodName("create");
        Class[] ParameterTypes = new Class[]{String.class};
        //UUID fileUuid, int blockNumber, int offset, int size
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUri};
        message.setParams(Parametes);
        message = communicate(message);

        return (SDFSFileChannel) message.getResult();

    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        Message message = new Message();
        message.setMethodName("closeReadonlyFile");
        Class[] ParameterTypes = new Class[]{UUID.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid};
        message.setParams(Parametes);
        message = communicate(message);

    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        Message message = new Message();
        message.setMethodName("closeReadwriteFile");
        Class[] ParameterTypes = new Class[]{UUID.class, int.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid, newFileSize};
        message.setParams(Parametes);
        message = communicate(message);

    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        Message message = new Message();

        message.setMethodName("mkdir");
        Class[] ParameterTypes = new Class[]{String.class};
        //UUID fileUuid, int blockNumber, int offset, int size
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUri};
        message.setParams(Parametes);
        message = communicate(message);


    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        Message message = new Message();
        message.setMethodName("addBlock");
        Class[] ParameterTypes = new Class[]{UUID.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid};
        message.setParams(Parametes);
        message = communicate(message);

        return (LocatedBlock) message.getResult();
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        Message message = new Message();
        message.setMethodName("addBlocks");
        Class[] ParameterTypes = new Class[]{UUID.class, int.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid, blockAmount};
        message.setParams(Parametes);
        message = communicate(message);

        return (List<LocatedBlock>) message.getResult();

    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        Message message = new Message();
        message.setMethodName("removeLastBlock");
        Class[] ParameterTypes = new Class[]{UUID.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid};
        message.setParams(Parametes);
        message = communicate(message);

    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        Message message = new Message();
        message.setMethodName("removeLastBlocks");
        Class[] ParameterTypes = new Class[]{UUID.class, int.class};
        //UUID fileUuid
        message.setParameterTypes(ParameterTypes);
        Object[] Parametes = new Object[]{fileUuid, blockAmount};
        message.setParams(Parametes);
        message = communicate(message);

    }
}
