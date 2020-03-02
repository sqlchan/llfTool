package com.sql.hbase.nosleep.mapReduce.topN;

import java.util.Map.Entry;
import java.util.TreeMap;

public class TreeMapDemo {
    public static void main(String[] args) {
        TreeMap<Long, Long> tree = new TreeMap<Long, Long>();
        tree.put(3333333L, 1333333L);
        tree.put(2222222L, 1222222L);
        tree.put(5555555L, 1555555L);
        tree.put(4444444L, 1444444L);
        for (Entry<Long, Long> entry : tree.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
        System.out.println("------------------------------------");
        System.out.println(tree.firstEntry().getValue()); //最小值
        System.out.println("------------------------------------");
        System.out.println(tree.lastEntry().getValue()); //最大值
        System.out.println("------------------------------------");
        System.out.println(tree.navigableKeySet()); //从小到大的正序key集合
        System.out.println("------------------------------------");
        System.out.println(tree.descendingKeySet());//从大到小的倒序key集合
    }
}

