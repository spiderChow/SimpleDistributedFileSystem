/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.filetree.*;
import sdfs.message.Message;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {

    public static void main(String args[]) throws IOException {
        NameNode nameNode = new NameNode(NAME_NODE_PORT);
        ServerSocket server = new ServerSocket(NAME_NODE_PORT);
        //server尝试接收其他Socket的连接请求，server的accept方法是阻塞式的
        while (true) {
            Socket socket = server.accept();
            //得到消息,解析消息
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            try {
                Message message = (Message) objectInputStream.readObject();
                nameNode.call(message);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
                // objectOutputStream.close();

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


    public static final int NAME_NODE_PORT = 4341;
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();
    private int[] blockNums = new int[10000];
    private int[] nodeNums = new int[10000];


    public NameNode(int nameNodePort) {
        //存储文件在Nodes文件夹中
        //检查是否创建好文件夹和root,否则则进行初始化
        File file = new File("Nodes");
        if (!file.exists()) {
            file.mkdir();
        }
        File root = new File("Nodes/0.node");
        if (!root.exists()) {
            DirNode rootDir = new DirNode();
            rootDir.setName("root");
            rootDir.setNodeNum(allocNewNodeNum());
            rootDir.dump(rootDir);
            File file2 = new File("Nodes/cgf.Cfg");
            try {
                file2.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            writeBlockNums();


        }


        File[] arr = file.listFiles();// 先列出当前文件夹下的文件及目录
        for (File ff : arr) {
            String num = ff.getName().substring(0, ff.getName().indexOf("."));

            if (!num.equals("cgf")) {
                int numm = Integer.parseInt(num);
                nodeNums[numm] = 1;
            } else {
                try {
                    FileInputStream fileInputStream = new FileInputStream(ff);
                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                    blockNums = (int[]) objectInputStream.readObject();
                    objectInputStream.close();
                    fileInputStream.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }


        }


        System.out.println(Arrays.toString(nodeNums));
        System.out.println(Arrays.toString(blockNums));


    }

    private void writeBlockNums() {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(new File("Nodes/cgf.Cfg"));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(blockNums);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        FileNode fileNode = pathToFileNode(fileUri);
        if (fileNode != null) {
            UUID uuid = UUID.randomUUID();
            readonlyFile.put(uuid, fileNode);
            //UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly
            SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, true);
            return sdfsFileChannel;

        } else {
            System.out.println(fileUri + "不存在");
        }
        return null;

    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        FileNode fileNode = pathToFileNode(fileUri);
        if (fileNode != null) {
            UUID uuid = UUID.randomUUID();
            readwritePFile.put(uuid, fileNode);
            //UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly
            SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
            return sdfsFileChannel;

        } else {
            System.out.println(fileUri + "不存在");
        }

        return null;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        System.out.println("NameNode enter create");


        int a = fileUri.lastIndexOf("/");
        DirNode fatherDirNode = null;
        if (a < 0) {
            fatherDirNode = pathToDirNode("");
        } else {
            fatherDirNode = pathToDirNode(fileUri.substring(0, a));
        }
        if (fatherDirNode != null) {
            System.out.println("存在路径,即将创建文件");

            //存在路径,创建文件
            String name = fileUri.substring(a + 1);
            FileNode fileNode = new FileNode();
            fileNode.setName(name);
            int aa = allocNewNodeNum();
            System.out.println("nodeNum " + aa);

            fileNode.setNodeNum(aa);
            fileNode.dump(fileNode);

            fatherDirNode.addEntry(new Entry(name, fileNode));//增加父亲的entry
            fatherDirNode.dump(fatherDirNode);

            //UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly
            UUID uuid = UUID.randomUUID();
            System.out.println("NameNode create uuid" + uuid.toString());
            readwritePFile.put(uuid, fileNode);
            System.out.println(readwritePFile.size());
            SDFSFileChannel sdfsFileChannel = new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
            return sdfsFileChannel;
        } else {
            //error:父路径不存在
            System.out.println(fileUri + "!路径不存在!");
        }


        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        //file size should be checked when closing the file.
        //FileNode fileNode=readonlyFile.get(fileUuid);

        readonlyFile.remove(fileUuid);

    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        FileNode fileNode = readonlyFile.get(fileUuid);
        fileNode.setFileSize(newFileSize);
        readwritePFile.remove(fileUuid);
    }

    @Override
    public void mkdir(String fileUri) throws IOException {

        int a = fileUri.lastIndexOf("/");
        DirNode fatherDirNode = null;
        if (a < 0) {
            fatherDirNode = pathToDirNode("");
        } else {
            fatherDirNode = pathToDirNode(fileUri.substring(0, a));
        }

        if (fatherDirNode != null) {
            //存在路径,创建子路径
            String name = fileUri.substring(a + 1);
            DirNode dirNode = new DirNode();
            dirNode.setName(name);
            dirNode.setNodeNum(allocNewNodeNum());
            dirNode.dump(dirNode);

            fatherDirNode.addEntry(new Entry(name, dirNode));//增加父亲的entry
            fatherDirNode.dump(fatherDirNode);
            //创建新的dirnode文件

        } else {
            //error:父路径不存在
            System.out.println(fileUri + "路径不存在!");
        }
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        System.out.println("NameNode addblock uuid" + fileUuid.toString());

        //遍历所有的LocatedBlock,取得一个可分配的LocatedBlock,
        //uuid是检查权限的!
        LocatedBlock locatedBlock = allocNewBlock();
        System.out.println(readwritePFile.size());
        FileNode fileNode = readwritePFile.get(fileUuid);

        BlockInfo blockInfo = new BlockInfo();
        blockInfo.addLocatedBlock(locatedBlock);
        fileNode.addBlockInfo(blockInfo);
        fileNode.dump(fileNode);

        return locatedBlock;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        ArrayList<LocatedBlock> blocks = new ArrayList<>();
        for (int i = 0; i < blockAmount; i++) {
            blocks.add(addBlock(fileUuid));
        }
        return blocks;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {

    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {

    }

    /*
    * search the locatedBlocks to find a new blocknum
    * and return a LocatedBlock with localhost and that blockNum
    * and add it to the locatedBlocks of NameNode
    * */
    private LocatedBlock allocNewBlock() {
        int blockNum = allocNewBlockNum();
        blockNums[blockNum] = 1;
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        LocatedBlock locatedBlock = new LocatedBlock(inetAddress, blockNum);
        return locatedBlock;

    }

    /*
    * search the nodes to find a new nodeNum for the uri
    * and return it
    * and add (it,uri) to the nodes of NameNode
    */
    private int allocNewNodeNum() {
        for (int i = 0; i < nodeNums.length; i++) {
            if (nodeNums[i] == 0) {
                nodeNums[i] = 1;
                return i;
            }
        }
        return -1;

    }

    private int allocNewBlockNum() {
        for (int i = 0; i < blockNums.length; i++) {
            if (blockNums[i] == 0) {
                blockNums[i] = 1;
                return i;
            }
        }
        return -1;

    }


    /*
    *  将Diruri变为DirNode
    *  需要DirUri必须存在
    *  用于create和makedir函数,去其path的前一部分
    * */
    private DirNode pathToDirNode(String DirUri) {
        System.out.println(DirUri);
        if (DirUri == null || DirUri.equals("")) {
            //root
            DirNode dirNode = new DirNode();
            dirNode.setNodeNum(0);
            dirNode = (DirNode) dirNode.load();
            return dirNode;
        }
        //检查输入的uri,改造为/开头
        if (DirUri.charAt(0) != '/') {
            DirUri = "/" + DirUri;
        }

        String[] pathNames = DirUri.split("/");
        DirNode nextNode = null;
        DirNode dirNode = new DirNode();//root
        dirNode.setNodeNum(0);
        dirNode = (DirNode) dirNode.load();
        for (int i = 0; i < pathNames.length - 1; i++) {

            nextNode = (DirNode) dirNode.retrieve(pathNames[i + 1]);
            System.out.println(pathNames[i + 1]);
            nextNode = (DirNode) nextNode.load();
            if (nextNode == null) {
                break;
            }
        }
        return nextNode;
    }


    /*
    * 根据文件的uri,若存在,则返回FileNode
    * 否则为null
    * */
    private FileNode pathToFileNode(String fileUri) {
        int a = fileUri.lastIndexOf("/");
        DirNode fatherDirNode;
        if (a < 0) {
            a = 0;
            fatherDirNode = pathToDirNode(fileUri.substring(0, a));
            a--;
        } else {
            fatherDirNode = pathToDirNode(fileUri.substring(0, a));
        }
        if (fatherDirNode != null) {
            //存在路径,寻找子路径
            String name = fileUri.substring(a + 1);
            System.out.println(name);
            FileNode fileNode = (FileNode) fatherDirNode.retrieve(name);
            fileNode = (FileNode) fileNode.load();
            return fileNode;
        }
        return null;
    }
}
