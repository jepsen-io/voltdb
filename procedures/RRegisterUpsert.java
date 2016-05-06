package jepsen.procedures;

import org.voltdb.*;

public class RRegisterUpsert extends VoltProcedure {
  public final SQLStmt upsert = new SQLStmt("UPSERT INTO rregisters (id, copy, value) VALUES (?, ?, ?);");

  public VoltTable[] run(long id, long[] copies, long value)
      throws VoltAbortException {
    for (long copy : copies) {
      voltQueueSQL(upsert, id, copy, value);
    }
    return voltExecuteSQL();
  }
}
