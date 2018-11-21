package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

public class EngineRace extends AbstractEngine {
    private Data[] datas;

    private TreeMap<Long,byte[]> cache = new TreeMap<>();
    //private LinkedHashMap<Long,byte[]> cacheL = new LinkedHashMap<>();
    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "init data file IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        data.storeKV(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        int offset = data.get(keyL);
        if (offset == 0) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found the value");
        }
        return data.readValue(offset);
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
        //单例的创建一个线程
        try {
            int[] range = SortIndex.instance.range(lower, upper);
            long tmp = -1L;
            for (int i = range[0]; i <= range[1]; i++) {
                long key = SortIndex.instance.get(i);
                if(key == Long.MAX_VALUE){
                    break;
                }
                if (tmp == key) {
                    continue;
                }
                tmp = key;
                //在这里停住
                byte[] value = cache.get(key);

                if(value == null){
                    synchronized (this){
                        byte[] bytes = cache.get(key);
                        if (bytes == null){
                            int modulus = (int) (key & (datas.length - 1));
                            Data data = datas[modulus];
                            int offset = data.get(key);
                            value =  data.readValue(offset);
                            if(cache.size() == 128){
                                cache.remove(cache.firstKey());
                            }
                            cache.put(key,value);
                        }else{
                            System.out.println(Thread.currentThread().getId() + "-加锁命中缓存 - key:" +key);
                            value = bytes;
                        }
                    }
                }else{
                    System.out.println(Thread.currentThread().getId() + "-直接命中缓存 - key:" +key);
                }
                byte[] value1 = new byte[Constant.VALUE_SIZE];
                System.arraycopy(value1,0,value,0,Constant.VALUE_SIZE);
                visitor.visit(ByteUtil.long2Bytes(key), value1);
            }
        } catch (EngineException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            EngineBoot.closeDataFile(datas);
        } catch (IOException e) {
            System.out.println("close file resource error");
        }
    }

    public Data[] getDatas() {
        return datas;
    }
}
