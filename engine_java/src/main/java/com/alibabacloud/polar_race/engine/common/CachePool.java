package com.alibabacloud.polar_race.engine.common;

import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author wangshuo
 * @version 2018-11-23
 */
public class CachePool {
    private Data[] datas;
    private volatile int readCursor; //装载 sortIndex 的游标
    private volatile int loadCursor; //装载 sortIndex 的游标
    private final LongObjectHashMap<byte[]>[] maps; //缓存数组，为了读写分离

    public CachePool(Data[] datas, int readCursor, int loadCursor,
            LongObjectHashMap<byte[]>[] maps) {
        this.datas = datas;
        this.readCursor = readCursor;
        this.loadCursor = loadCursor;
        this.maps = maps;
    }

    public Data[] getDatas() {
        return datas;
    }

    public void setDatas(Data[] datas) {
        this.datas = datas;
    }

    public int getReadCursor() {
        return readCursor;
    }

    public void setReadCursor(int readCursor) {
        this.readCursor = readCursor;
    }

    public int getLoadCursor() {
        return loadCursor;
    }

    public void setLoadCursor(int loadCursor) {
        this.loadCursor = loadCursor;
    }

    public LongObjectHashMap<byte[]>[] getMaps() {
        return maps;
    }
}
