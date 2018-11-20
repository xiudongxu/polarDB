package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongxu.xiu
 * @since 2018-11-19 下午6:03
 */
public class SortIndex {

    public static SortIndex instance = new SortIndex();

    private volatile AtomicInteger atomicInteger;
    private volatile long[] index;

    private SortIndex(){
        this.atomicInteger = new AtomicInteger(0);
        index = new long[64000000];
        for (int i = 0; i < index.length; i++) {
            index[i] = Long.MAX_VALUE;
        }
    }

    public void set(long key){
        index[atomicInteger.getAndIncrement()] = key;
    }

    public void set(long key,int id){
        index[id] = key;
    }

    public int[] range(byte[] lower,byte[] upper) {
        int[] ints = new int[2];
        ints[0] = lower == null ? 0 : binarySearch(index, ByteUtil.bytes2Long(lower));
        ints[1] = upper == null ? index.length-1 : binarySearch(index, ByteUtil.bytes2Long(upper));
        return ints;
    }

    public long get(int i){
        return index[i];
    }
    public void sort(){
        Arrays.sort(index);
    }

    public int getIndex(long key){
        return binarySearch(index,key);
    }

    public void removeDuplicate(){
        long tmp = index[0];
        for (int i = 1; i < index.length; i++) {
            if(tmp == index[i]){
                index[i] = Long.MAX_VALUE;
            }else{
                tmp = index[i];
            }
        }
    }
    private static int binarySearch(long[] arr, long x) {
        int low = 0;
        int high = arr.length - 1;
        int mid;
        while (low <= high) {
            mid = (low + high) / 2;
            if (arr[mid] > x) {
                high = mid - 1;
            } else if (arr[mid] < x) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }
}
