package com.alibabacloud.polar_race.engine.common.cache;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CachePool {
    private int readCursor;
    private int loadCursor;
    private CacheBlock[] blocks;

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
