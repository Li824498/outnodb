package server.dm;

import com.google.common.primitives.Bytes;
import server.dm.dataItem.DataItem;
import server.dm.dataItem.DataItemImpl;
import server.dm.logger.Logger;
import server.dm.page.Page;
import server.dm.page.PageX;
import server.dm.pageCache.PageCache;
import server.tm.TransactionManager;
import server.utils.Panic;
import server.utils.Parser;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;

/**
 * 从日志文件中的data部分（这一块就是日志），来获取日志
 * InsertLog [LogType][XID][Pgno][Offset][Raw]
 * UpdateLog [LogType][XID][UID][OldRaw][NewRaw]
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int OF_XID = 1;
    private static final int LEN_XID = 8;

    private static final int OF_PGNO = OF_XID + LEN_XID;
    private static final int LEN_PGNO = 4;
    private static final int OF_OFFSET = OF_PGNO + LEN_PGNO;
    private static final int LEN_OFFSET = 2;
    private static final int OF_INSERT_RAW = OF_OFFSET + LEN_OFFSET;

    private static final int OF_UID = OF_XID + LEN_XID;
    private static final int LEN_UID = 8;
    private static final int OF_UPDATE_RAW = OF_UID + LEN_UID;

    public static byte[] updateLog(Long xid, DataItemImpl di) {
        byte[] logTypeBytes = new byte[]{LOG_TYPE_UPDATE};
        byte[] xidBytes = Parser.long2byte(xid);
        byte[] uidBytes = Parser.long2byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        byte[] newRaw = di.getRaw().getData();
        return Bytes.concat(logTypeBytes, xidBytes, uidBytes, oldRaw, newRaw);
    }

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeBytes = new byte[]{LOG_TYPE_INSERT};
        byte[] xidBytes = Parser.long2byte(xid);
        byte[] pgnoBytes = Parser.int2byte(pg.getPageNumber());
        byte[] offsetBytes = Parser.short2byte(PageX.getFSO(pg));
        byte[] rawBytes = raw;
        return Bytes.concat(logTypeBytes, xidBytes, pgnoBytes, offsetBytes, rawBytes);
    }


    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {

        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();

            if(log == null) {
                break;
            }
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo iInfo = parserByte2InsetLogInfo(log);
                pgno = iInfo.pgno;
            } else {
                UpdateLogInfo uInfo = parserByte2UpdateLogInfo(log);
                pgno = uInfo.pgno;
            }
            maxPgno = Math.max(pgno, maxPgno);
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }

        pc.truncateyByPgno(maxPgno);
        System.out.println("删除未记录log的数据完毕");

        redoRedoLog(tm, lg, pc);
        System.out.println("重放log（redolog）完毕");

        redoUndoLog(tm, lg, pc);
        System.out.println("重放log（undolog）完毕");


        System.out.println("Recover完毕");
    }



    private static UpdateLogInfo parserByte2UpdateLogInfo(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        // todo 检查其他文件可能存在的数组长度错误
        updateLogInfo.xid = Parser.byte2long(Arrays.copyOfRange(log, OF_XID, OF_XID + LEN_XID));
        byte[] uid = Arrays.copyOfRange(log, OF_UID, OF_UID + LEN_UID);
        updateLogInfo.pgno = (int) (ByteBuffer.wrap(uid).getLong() >>> 32);
        updateLogInfo.offset = (short) (ByteBuffer.wrap(uid).getLong() & ((1 << 16) - 1));
        int rawLength = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + rawLength);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + rawLength, OF_UPDATE_RAW + rawLength * 2);
        return updateLogInfo;
    }

    private static InsertLogInfo parserByte2InsetLogInfo(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.byte2long(Arrays.copyOfRange(log, OF_XID, OF_XID + LEN_XID));
        insertLogInfo.pgno = Parser.byte2int(Arrays.copyOfRange(log, OF_PGNO, OF_PGNO + LEN_PGNO));
        insertLogInfo.offset = Parser.byte2short(Arrays.copyOfRange(log, OF_OFFSET, OF_OFFSET + LEN_OFFSET));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static void redoRedoLog(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while(true) {
            byte[] log = lg.next();

            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parserByte2InsetLogInfo(log);
                if(tm.isActive(insertLogInfo.xid)) continue;

                redoInsertLog(insertLogInfo, pc);

            } else {
                UpdateLogInfo updateLogInfo = parserByte2UpdateLogInfo(log);
                if(tm.isActive(updateLogInfo.xid)) continue;

                redoUpdateLog(updateLogInfo, pc);
            }

        }
    }

    private static void redoInsertLog(InsertLogInfo insertLogInfo, PageCache pc) {
        int pgno = insertLogInfo.pgno;
        short offset = insertLogInfo.offset;

        Page page = null;
        try {
            page = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverInsert(page, insertLogInfo.raw, offset);
        } finally {
            page.release();
        }

    }

    private static void redoUpdateLog(UpdateLogInfo updateLogInfo, PageCache pc) {
        int pgno = updateLogInfo.pgno;
        short offset = updateLogInfo.offset;

        Page page = null;
        try {
            page = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page, updateLogInfo.newRaw, offset);
        } finally {
            page.release();
        }
    }

    private static void redoUndoLog(TransactionManager tm, Logger lg, PageCache pc) {
        // 只能倒序处理，倒着来拆
        Deque<byte[]> stack = new LinkedList<>();

        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            stack.push(log);
        }

        while(!stack.isEmpty()) {
            byte[] log = stack.pop();
            if(isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parserByte2InsetLogInfo(log);
                if(!tm.isActive(insertLogInfo.xid)) {
                    continue;
                }
                undoInsertLog(insertLogInfo, pc);
            } else {
                UpdateLogInfo updateLogInfo = parserByte2UpdateLogInfo(log);
                if(!tm.isActive(updateLogInfo.xid)) {
                    continue;
                }
                undoUpdateLog(updateLogInfo, pc);
            }
        }
    }

    private static void undoInsertLog(InsertLogInfo insertLogInfo, PageCache pc) {
        DataItem.setDataItemRawInvalid(insertLogInfo.raw);
        redoInsertLog(insertLogInfo, pc);

    }

    private static void undoUpdateLog(UpdateLogInfo updateLogInfo, PageCache pc) {
        int pgno = updateLogInfo.pgno;
        short offset = updateLogInfo.offset;

        Page page = null;
        try {
            page = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page, updateLogInfo.oldRaw, offset);
        } finally {
            page.release();
        }
    }
}
