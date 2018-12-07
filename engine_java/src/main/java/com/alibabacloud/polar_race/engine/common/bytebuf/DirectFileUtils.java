package com.alibabacloud.polar_race.engine.common.bytebuf;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;

public class DirectFileUtils {

    private static int fcblockSize;
    public static DirectIOLib directIOLib = DirectIOLib.INSTANCE;

    public static void initBlockSize(String path){
        final int _PC_REC_XFER_ALIGN = 0x11;
        fcblockSize = directIOLib.pathconf(path, _PC_REC_XFER_ALIGN);
        fcblockSize = lcm(fcblockSize, directIOLib.getpagesize());
    }

    public static long consAdrress(int capacity){
        NativeLong blockSize = new NativeLong(fcblockSize);
        PointerByReference pointerToPointer = new PointerByReference();
        // align memory for use with O_DIRECT
        directIOLib.posix_memalign(pointerToPointer, blockSize, new NativeLong(capacity));
        try {
            Field field = Pointer.class.getDeclaredField("peer");
            field.setAccessible(true);
            return field.getLong(pointerToPointer.getValue());
        } catch (IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int lcm(long x, long y) {
        long g = x; // will hold gcd
        long yc = y;

        // get the gcd first
        while (yc != 0) {
            long t = g;
            g = yc;
            yc = t % yc;
        }

        return (int)(x*y/g);
    }

    public static int openReadOnly(String path) throws IOException {
        int fd = directIOLib.open(path, DirectIOLib.O_RDONLY | DirectIOLib.O_DIRECT, 00644);
        if (fd < 0){
            throw new IOException("open file error path = " + path);
        }
        return fd;
    }

    public static int pread(int fd, ByteBuffer buf, long offset) throws IOException {
        buf.clear();
        final long address =  ((DirectBuffer)buf).address();
        Pointer pointer = new Pointer(address);
        int n = directIOLib.pread(fd, pointer, new NativeLong(buf.capacity()), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error reading file at offset " + offset + ": " + getLastError());
        }
        return n;
    }


    public static String getLastError() {
        return directIOLib.strerror(Native.getLastError());
    }

    public void closeFile(int fd){
        directIOLib.close(fd);
    }
}