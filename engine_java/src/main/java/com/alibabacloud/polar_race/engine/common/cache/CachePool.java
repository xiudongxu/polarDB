package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Data;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CachePool {
    private int readCursor;
    private int loadCursor;

    private Data[] datas;
    private CacheBlock[] blocks;

    public CachePool(Data[] datas, CacheBlock[] blocks) {
        this.datas = datas;
        this.blocks = blocks;
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

    public CacheBlock[] getBlocks() {
        return blocks;
    }

    public void setBlocks(CacheBlock[] blocks) {
        this.blocks = blocks;
    }
}
