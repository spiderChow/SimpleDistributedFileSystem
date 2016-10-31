package sdfs.test;

import sdfs.datanode.DataNode;
import sdfs.message.Message;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

/**
 * Created by shiyuhong on 16/10/23.
 */
public class Client {

    public static void main(String args[]) throws Exception {

        UUID fileUuid = UUID.randomUUID();

        //read data from ip: LocatedBlock.inetAddress ;blockNum: LocatedBlcok.blockNumber
        byte[] ret = {'a', 's', 'd'};
        String host = "127.0.0.1";  //要连接的服务端IP地址
        int port = DataNode.DATA_NODE_PORT;   //要连接的服务端对应的监听端口
        //与服务端建立连接
        Socket client = null;
        try {
            client = new Socket(host, port);

            //marshal the message
            Message message = new Message();
            message.setMethodName("write");//函数名
            Class[] ParameterTypes = new Class[]{UUID.class, int.class, int.class, byte[].class};//参数类型
            message.setParameterTypes(ParameterTypes);
            Object[] Parametes = new Object[]{fileUuid, 1, 0, ret};//参数
            message.setParams(Parametes);
            //通过Socket建立连接,将message写入通道.
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(client.getOutputStream());
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            // objectOutputStream.close();
            while (true) {
                //从服务器读取响应
                ObjectInputStream objectInputStream = new ObjectInputStream(client.getInputStream());
                try {
                    message = (Message) objectInputStream.readObject();
                    System.out.println("Client" + message.getMethodName());
                    objectInputStream.close();
                    break;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            objectOutputStream.close();
            client.close();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}