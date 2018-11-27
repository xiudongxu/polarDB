package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongxu.xiu
 * @since 2018-11-19 下午6:03
 */
public class SortIndex {

    public static SortIndex instance = new SortIndex();
    private volatile AtomicInteger atomicInteger;
    private volatile byte[][] index;
    private Comparator<byte[]> cmp = (o1, o2) -> {
        int length = o1.length;
        for (int i = 0; i < length; i++) {
            int thisByte = o1[i] & 0xff;
            int thatByte = o2[i] & 0xff;
            if (thisByte != thatByte) {
                return thisByte - thatByte;
            }
        }
        return 0;
    };

    private SortIndex() {
        this.atomicInteger = new AtomicInteger(0);
        index = new byte[Constant.TOTAL_KV_COUNT][Constant.KEY_SIZE];
        Arrays.fill(index, Long.MIN_VALUE);
    }

    public void set(byte[] key){
        index[atomicInteger.getAndIncrement()] = key;
    }

    public byte[] get(int i){
        return index[i];
    }
    public void sort(){
        Arrays.sort(index, cmp);
    }

    /*public static void main(String[] args) {
        Comparator<byte[]> cmp = (o1, o2) -> {
            int length = o1.length;
            for (int i = 0; i < length; i++) {
                int thisByte = o1[i] & 0xff;
                int thatByte = o2[i] & 0xff;
                if (thisByte != thatByte) {
                    return thisByte - thatByte;
                }
            }
            return 0;
        };
        byte[][] bytes = new byte[6][2];
        bytes[0] = new byte[]{(byte)0x80, -5};
        bytes[1] = new byte[]{(byte)0x80, -8};
        bytes[2] = new byte[]{(byte)0x00, 0};
        bytes[3] = new byte[]{(byte)0x00, 7};
        bytes[4] = new byte[]{(byte)0x00, 8};
        bytes[5] = new byte[]{(byte)0x00, 5};

        Arrays.sort(bytes, cmp);
        for (int i = 0; i < 6; i++) {
            System.out.println(bytes[i]);
        }
    }*/


    /*public int[] range(byte[] lower,byte[] upper, int totalKvCount) {
        int[] ints = new int[2];
        ints[0] = lower == null ? 0 : binarySearch(index, ByteUtil.bytes2Long(lower));
        ints[1] = upper == null ? totalKvCount : binarySearch(index, ByteUtil.bytes2Long(upper));
        return ints;
    }

    private static int binarySearch(long[] arr, long x) {
        int low = 0;
        int high = arr.length - 1;
        int mid = 0;
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
        return mid; //返回与被查找数据最近的且大于它的值
    }*/

    public byte[][] getIndex() {
        return index;
    }
}
