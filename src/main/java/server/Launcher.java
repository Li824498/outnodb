package server;

import common.Error;
import org.apache.commons.cli.*;
import server.dm.DataManager;
import server.tbm.TableManager;
import server.tm.TransactionManager;
import server.tm.TransactionManagerImpl;
import server.utils.Panic;
import server.vm.VersionManager;
import server.vm.VersionManagerImpl;

public class Launcher {

    public static final int port = 9999;

    public static final long DEFAUlT_MEM = (1<<20)*64;


    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("input must be (open | create)");
    }

    public static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAUlT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    public static void openDB(String path, long mem) {
        TransactionManagerImpl tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, DEFAUlT_MEM, tm);
        VersionManagerImpl vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFAUlT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException());
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(Error.InvalidMemException());
        }
        return DEFAUlT_MEM;
    }
}
