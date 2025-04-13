package common;

public class Error {
    public static Exception NullEntryException = new RuntimeException("Null entry!");
    public static Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");

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

    public static Exception BadLogFileException() {
        return new RuntimeException("Bad Log File!");

    }

    public static Exception DataTooLargeException() {
        return new RuntimeException("Data too large!");
    }

    public static Exception DataBaseBusyException() {
        return new RuntimeException("DataBase is too busy!");
    }

    public static Exception DeadLockException() {
        return new RuntimeException("Has Dead Lock!");
    }
}
