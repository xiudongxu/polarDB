package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午4:24
 */
public class MappedFile {

    private MappedByteBuffer mappedByteBuffer;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    public MappedFile(String fileName, long position, long maxSize) throws IOException {
        File file = new File(fileName);
        if(!file.exists()){
            file.createNewFile();
        }
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, maxSize);
    }

    public void cleanMappedBuffer(){
        clean(mappedByteBuffer);
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public void close() throws IOException {
        randomAccessFile.close();
        randomAccessFile = null;
        fileChannel = null;
        clean(mappedByteBuffer);
        mappedByteBuffer = null;
    }

    public void openNewMap(long position, long maxSize) throws EngineException {
        try {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, maxSize);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,"重新分配映射出错");
        }
    }
    public static void clean(final ByteBuffer buffer) {
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }
}
