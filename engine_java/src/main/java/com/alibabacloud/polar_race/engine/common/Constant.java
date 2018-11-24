package com.alibabacloud.polar_race.engine.common;

/**
 * @author wangshuo
 * @version 2018-11-09
 */
public interface Constant {

    //指向文件的指针 pointer 是一个long数据，高四字节表示数据文件编号；低位四字节表示数据文件中value的偏移量
    //int TOTAL_KV_COUNT = 64000000; //总键值对个数
    int TOTAL_KV_COUNT = 6400; //总键值对个数
    byte DATA_FILE_COUNT = 64; //用于存储值的文件个数

    int INIT_MAP_CAP = TOTAL_KV_COUNT / DATA_FILE_COUNT; //初始索引 map 的容量

    short VALUE_SIZE = 4096;
    short KEY_SIZE = 8;

    int ONE_LOAD_SIZE = 1024 * 1024 * 6; //一次加载的字节数  1024*1024

    int KEY_MAPPED_SIZE = 12 * 1000000;

    int THREAD_COUNT = 64;

    int CACHE_SIZE = 20;

    int TOTAL_CACHE_COUNT = THREAD_COUNT * CACHE_SIZE;

    int ONE_CACHE_SIZE = THREAD_COUNT * CACHE_SIZE;

    int CACHE_CAP = THREAD_COUNT * CACHE_SIZE * 2;

    int MAPS_PER_BLOCK = 2;
}
