/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.*;

public abstract class Node implements Serializable {
    private static final long serialVersionUID = 42L;
    private String name;
    private int nodeNum;
    private boolean isfile;

    public Node() {
    }

    public void dump(Node node) {
        //序列化操作1--FileOutputStream
        ObjectOutputStream oos1 = null;
        try {
            oos1 = new ObjectOutputStream(new FileOutputStream("Nodes/"+getNodeNum() + ".node"));
            oos1.writeObject(node);
            System.out.print("dump");
            oos1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Node load() {
        ObjectInputStream ois1 = null;
        try {
            ois1 = new ObjectInputStream(new FileInputStream("Nodes/"+getNodeNum() + ".node"));
            Node node = (Node) ois1.readObject();
            ois1.close();
            return node;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
    }

    public boolean isfile() {
        return isfile;
    }

    public void setIsfile(boolean isfile) {
        this.isfile = isfile;
    }
    //只有DirNode 和 fileNode之分


}