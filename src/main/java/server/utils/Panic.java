package server.utils;

public class Panic {
    public static void panic(Exception error) {
        error.printStackTrace();
        System.exit(1);
    }
}
