package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CacheBlock {
    private LongObjectHashMap<byte[]>[] maps = new LongObjectHashMap[Constant.MAPS_PER_BLOCK];

    public CacheBlock() {
        for (int i = 0; i < Constant.MAPS_PER_BLOCK; i++) {
            maps[i] = new LongObjectHashMap<>(Constant.BLOCK_SIZE);
        }
    }

    public LongObjectHashMap<byte[]>[] getMaps() {
        return maps;
    }

    public void setMaps(LongObjectHashMap<byte[]>[] maps) {
        this.maps = maps;
    }
}
