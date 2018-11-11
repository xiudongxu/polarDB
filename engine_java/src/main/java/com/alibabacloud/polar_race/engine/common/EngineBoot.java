package com.alibabacloud.polar_race.engine.common;

import java.io.IOException;

/**
 * @author wangshuo
 * @version 2018-11-10
 */
public class EngineBoot {

    public static Data[] initDataFile(String path) throws IOException {
        Data[] datas = new Data[Constant.DATA_FILE_COUNT];
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            datas[i] = new Data(path, i);
        }
        return datas;
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}
