package com.alibabacloud.polar_race.engine.common;

/**
 * @author wangshuo
 * @version 2018-11-09
 */
public interface Constant {

    int TOTAL_KV_COUNT = 64000000; //总键值对个数
    byte DATA_FILE_COUNT = 64; //用于存储值的文件个数

    int INIT_MAP_CAP = TOTAL_KV_COUNT / DATA_FILE_COUNT; //初始索引 map 的容量

    short VALUE_SIZE = 4096;
    short KEY_SIZE = 8;

    int ONE_LOAD_SIZE = 1024 * 1024 * 6; //一次加载的字节数  1024*1024

    int KEY_MAPPED_SIZE = 12 * 1000000;

    byte THREAD_COUNT = 64;

    int SLOT_COUNT = 2048;

    int SLOT_SIZE = 64;

    int SLOT_BUF_CAP = SLOT_SIZE * VALUE_SIZE;

    int BYTE_BUF_CAP = SLOT_COUNT * SLOT_SIZE * VALUE_SIZE;
}
