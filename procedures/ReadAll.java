package jepsen.procedures;

import org.voltdb.*;

public class ReadAll extends VoltProcedure {
  public final SQLStmt readAll = new SQLStmt("SELECT * FROM registers;");

  public VoltTable run() throws VoltAbortException {
    voltQueueSQL(readAll);
    return voltExecuteSQL()[0];
  }
}
