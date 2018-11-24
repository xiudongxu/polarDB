package com.alibabacloud.polar_race.engine.common.cache;

import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CacheBlock {
    private LongObjectHashMap<byte[]>[] maps;

    public LongObjectHashMap<byte[]>[] getMaps() {
        return maps;
    }

    public void setMaps(LongObjectHashMap<byte[]>[] maps) {
        this.maps = maps;
    }
}
