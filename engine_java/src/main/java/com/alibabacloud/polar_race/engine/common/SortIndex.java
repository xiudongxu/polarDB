package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongxu.xiu
 * @since 2018-11-19 下午6:03
 */
public class SortIndex {

    public static SortIndex instance = new SortIndex();

    private AtomicInteger atomicInteger;
    private long[] index;

    private SortIndex(){
        this.atomicInteger = new AtomicInteger(0);
        index = new long[64000000];
    }

    public void set(long key){
        index[atomicInteger.getAndIncrement()] = key;
    }

    public int[] range(byte[] lower,byte[] upper){
        int[] ints = new int[2];
        if(lower == null){
            ints[0] = 0;
        }
        if(upper == null){
            ints[1] = index.length - 1 ;
        }
        return ints;

//        long start = ByteUtil.bytes2Long(lower);
//        long end = ByteUtil.bytes2Long(upper);
//
//        for (int i = 0; i < index.length; i++) {
//            if (index[i] == start){
//                ints[0] = i;
//            }
//            if (index[i] == end){
//                ints[1] = i;
//                break;
//            }
//        }
//        return ints;
    }

    public long get(int i){
        return index[i];
    }
    public void sort(){
        Arrays.sort(index);
    }
}
