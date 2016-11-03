/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private final List<BlockInfo> blockInfos = new ArrayList<>();
    private int fileSize;//file size should be checked when closing the file.

    public FileNode() {
        setIsfile(true);
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);

    }

    public void removeLastBlockInfo() {
        blockInfos.remove(blockInfos.size() - 1);

    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;

    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }

    public int getBlockAmount() {
        return blockInfos.size();
    }

    public void dump(FileNode node) {
        //序列化操作1--FileOutputStream
        ObjectOutputStream oos1 = null;
        try {
            oos1 = new ObjectOutputStream(new FileOutputStream("Nodes/" + getNodeNum() + ".node"));
            oos1.writeObject(node);
            System.out.println("dump" + getName() + getNodeNum());
            oos1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public FileNode load() {
        ObjectInputStream ois1 = null;
        try {
            ois1 = new ObjectInputStream(new FileInputStream("Nodes/" + getNodeNum() + ".node"));
            FileNode node = (FileNode) ois1.readObject();
            ois1.close();
            return node;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }

    public BlockInfo getBlockInfoAt(int a) {
        return blockInfos.get(a);
    }


}

