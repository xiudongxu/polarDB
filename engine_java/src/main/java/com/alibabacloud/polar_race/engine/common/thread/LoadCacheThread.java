package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author wangshuo
 * @version 2018-11-23
 */
public class LoadCacheThread extends Thread {

    private int cursor; //sortIndex 的游标
    private Data[] datas;
    private LongObjectHashMap<byte[]>[] maps; //缓存数组，为了读写分离

    public LoadCacheThread(int cursor, Data[] datas, LongObjectHashMap<byte[]>[] maps) {
        this.cursor = cursor;
        this.datas = datas;
        this.maps = maps;
    }

    @Override
    public void run() {
        while (true) {
            if (cursor >= Constant.TOTAL_KV_COUNT) break;
            int mapIndex = (cursor / Constant.CACHE_SIZE) & Constant.POOL_COUNT - 1;
            for (int i = cursor; i < cursor + Constant.CACHE_SIZE; i++) {
                long key = SortIndex.instance.get(i);
                int modulus = (int) (key & (datas.length - 1));
                Data data = datas[modulus];

                try {
                    byte[] bytes = data.readValue(data.get(key));
                    maps[mapIndex].put(key, bytes);
                } catch (EngineException e) {
                    System.out.println("during load cache : read value IO exception!!!");
                }
            }
            try {
                maps.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
