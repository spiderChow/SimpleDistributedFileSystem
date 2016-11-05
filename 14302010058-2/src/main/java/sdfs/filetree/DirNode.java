/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
    private static final long serialVersionUID = 8178778592344231767L;
    private final Set<Entry> entries = new HashSet<>();


    public  DirNode(){
        setIsfile(false);
    }


    @Override
    public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    public boolean addEntry(Entry entry) {
        return entries.add(entry);
    }

    public boolean removeEntry(Entry entry) {
        return entries.remove(entry);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode entries1 = (DirNode) o;

        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public Node retrieve(String pathName) {
        Iterator<Entry> iterator = iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            if (entry.hashCode() == pathName.hashCode()) {
                return entry.getNode();
            }
        }
        return null;
    }

    public void dump(DirNode node) {
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

    public DirNode load() {
        ObjectInputStream ois1 = null;
        try {
            ois1 = new ObjectInputStream(new FileInputStream("Nodes/"+getNodeNum() + ".node"));
            DirNode node = (DirNode) ois1.readObject();
            ois1.close();
            return node;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }


}
