/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.message.Message;
import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {
    public static void main(String args[]) throws IOException {
        DataNode dataNode = new DataNode();
        ServerSocket server = new ServerSocket(DATA_NODE_PORT);
        //server尝试接收其他Socket的连接请求，server的accept方法是阻塞式的
        while (true) {
            //server尝试接收其他Socket的连接请求，server的accept方法是阻塞式的
            Socket socket = server.accept();
            //得到消息,解析消息
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            try {
                Message message = (Message) objectInputStream.readObject();
                dataNode.call(message);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            objectInputStream.close();
            socket.close();
        }
    }

    public void call(Message message) {
        // TODO Auto-generated method stub
        // Object object = ServiceEntry.get(message.getInterfaces().getName());
        System.out.println("call" + message.getMethodName());
        try {
            Method method = this.getClass().getMethod(message.getMethodName(), message.getParameterTypes());
            Object result = method.invoke(this, message.getParams());
            message.setResult(result);
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */
    public static final int BLOCK_SIZE = 64 * 1024;
    public static final int DATA_NODE_PORT = 4342;
    //    put off due to its difficulties
    //    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();
    public DataNode() {
        File file = new File("blocks");
        if (!file.exists()) {
            file.mkdir();
        }
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        //Read data from a block.
        byte[] data = new byte[size];
        File file = new File("Blocks/" + blockNumber + ".block");
        FileInputStream fileInputStream = new FileInputStream(file);
        fileInputStream.read(data, offset, size);
        return data;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        //Write data to a block.
        File file = new File("Blocks/" + blockNumber + ".block");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(b, offset, b.length);
    }
}
