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

    public static Exception InvalidCommandException() {
        return new RuntimeException("Invalid command!");
    }

    public static Exception TableNoIndexException() {
        return new RuntimeException("Table has no index!");
    }

    public static Exception DuplicatedTableException() {
        return new RuntimeException("Duplicated table!");
    }

    public static Exception TableNotFoundException() {
        return new RuntimeException("Table not found");
    }

    public static Exception InvalidValuesException() {
        return new RuntimeException("Value invalid");
    }

    public static Exception FieldNotIndexedException() {
        return new RuntimeException("Not Index!");
    }

    public static Exception FieldNotFoundException() {
        return new RuntimeException("Field not found");
    }

    public static Exception InvalidLogOpException() {
        return new RuntimeException("LogOp is Invalid");
    }

    public static Exception InvalidPkgDataException() {
        return new RuntimeException("Package is invalid");
    }

    public static Exception InvalidMemException() {
        return new RuntimeException("Invalid mem num !");
    }
}
