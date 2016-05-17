package jepsen.procedures;

import org.voltdb.*;

public class SRegisterStrongRead extends VoltProcedure {
  // Never used, but forces the static analyzer to flag this procedure as a
  // write
  public final SQLStmt insert = new SQLStmt("INSERT INTO registers (id, value) VALUES (-1, -1)");

  public final SQLStmt read = new SQLStmt("SELECT * FROM registers WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(read, id);
    return voltExecuteSQL();
  }
}
