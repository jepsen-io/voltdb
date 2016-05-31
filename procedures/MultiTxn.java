package jepsen.procedures;

import org.voltdb.*;

// Processes arbitrary r/w k/v transactions
public class MultiTxn extends VoltProcedure {
  public final SQLStmt write = new SQLStmt("UPDATE multi SET value = ? WHERE system = ? AND key = ?");
  public final SQLStmt read = new SQLStmt("SELECT * FROM multi WHERE system = ? AND key = ?");

  // Arrays of the function, key, and value for each op in the transaction.
  // We assume string keys and integer values.
  public VoltTable[] run(int system, String[] fs, String[] ks, int[] vs) {
    assert fs.length == ks.length && ks.length == vs.length;

    for (int i = 0; i < fs.length; i++) {
      if (fs[i].equals("read")) {
        voltQueueSQL(read, system, ks[i]);
      } else if (fs[i].equals("write")) {
        voltQueueSQL(write, vs[i], system, ks[i]);
      } else {
        throw new IllegalArgumentException("Don't know how to interpret op " + fs[i]);
      }
    }
    return voltExecuteSQL();
  }
}
