package presto;

import it.unimi.dsi.fastutil.ints.*;

public class Fast {
    public static void main(String[] args) {
        //===========IntList
        IntList list = new IntArrayList();

        for(int i = 0; i < 100; i++){
            list.add(i);
        }

//取值
        int value = list.getInt(0);
        System.out.println(value);// 0

//转成数组
        int[] values = list.toIntArray();
        System.out.println(values.length);// 1000

//遍历
        IntListIterator i = (IntListIterator) list.iterator();
        while(i.hasNext()){
            System.out.println(i.nextInt());
        }

//===========Int2BooleanMap
        Int2BooleanMap map = new Int2BooleanArrayMap();

        map.put(1, true);
        map.put(2, false);

//取值
        boolean value1 = map.get(1);
        boolean value2 = map.get(2);

        System.out.println(value1);// true
        System.out.println(value2);// false

//===========IntSortedSet
        IntSortedSet s = new IntLinkedOpenHashSet( new int[] { 4, 3, 2, 1 } );
//获取第一个元素
        System.out.println(s.firstInt()); // 4
//获取最后一个元素
        System.out.println(s.lastInt()); // 1
//判断是否包含一个元素
        System.out.println(s.contains(5)); // false
    }
}
