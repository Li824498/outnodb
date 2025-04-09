package server.utils;

public class Types {
    public static long addressToUid(int pageNumber, int offset) {
        long l0 = (short) pageNumber;
        long l1 = (short) offset;
        return l0 << 32 | l1;
    }
}
