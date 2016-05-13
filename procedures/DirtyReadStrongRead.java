package jepsen.procedures;

import org.voltdb.*;

public class DirtyReadStrongRead extends VoltProcedure {
  // Never used, but forces the static analyzer to flag this procedure as a
  // write
  public final SQLStmt insert = new SQLStmt("INSERT INTO dirty_reads (id) VALUES (-1)");

  public final SQLStmt read = new SQLStmt("SELECT * FROM dirty_reads ORDER BY id ASC;");

  public VoltTable[] run() throws VoltAbortException {
    voltQueueSQL(read);
    return voltExecuteSQL();
  }
}
