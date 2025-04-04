package common;

public class Error {
    public static RuntimeException FileExistsException() {
        return new RuntimeException("File already exists!");
    }

    public static RuntimeException FileCannotRWException() {
        return new RuntimeException("File cannot read or write!");
    }

    public static RuntimeException FileNotExistsException() {
        return new RuntimeException("File not exists!");
    }

    public static RuntimeException BadXidFileException() {
        return new RuntimeException("Bad Xid File!");
    }
}
