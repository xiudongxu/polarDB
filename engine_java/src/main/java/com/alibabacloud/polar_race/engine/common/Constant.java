package com.alibabacloud.polar_race.engine.common;

/**
 * @author wangshuo
 * @version 2018-11-09
 */
public interface Constant {

    //指向文件的指针 pointer 是一个int数据，高位一字节表示数据文件编号；低位三字节表示数据文件中value的偏移量
    //每个指针最多能表示 2^23-1 个 value，如果数据分配在各个文件的数量不均匀，其中有超过该值的将会报错
    //因此 DATA_FILE_COUNT 的值慎重调整，会影响 pointer 指针的表示
    int TOTAL_KV_COUNT = 64000000; //总键值对个数
    byte DATA_FILE_COUNT = 64; //用于存储值的文件个数

    int MAX_OFFSET = 8388607; //2^23 - 1
    int INIT_MAP_CAP = TOTAL_KV_COUNT / DATA_FILE_COUNT; //初始索引 map 的容量

    byte ACCESS_FILE_COUNT = 2; //访问文件的channel数

    byte INDEX_MARK_SIZE = 4;
    byte INDEX_SIZE = 12; //索引大小：key(8B) + pointer(4B) = 12

    byte VALUE_MARK_SIZE = 4;
    short VALUE_SIZE = 4096;
}
