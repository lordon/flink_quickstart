package org.myorg.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class TableJob2 {
  private static ExecutionEnvironment executionEnvironment;
  private static BatchTableEnvironment batchTableEnvironment;

  /**
   * Example which demonstrates that a MULTISET (e.g result of COLLECT) inside of a JOINed table is
   * not working
   * <p>
   *
   * SELECT a, d
   * FROM TableA JOIN (
   *   SELECT b, COLLECT(c) AS d
   *   FROM TableB
   *   GROUP BY b
   * ) TableC ON a = b
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    executionEnvironment = ExecutionEnvironment.createLocalEnvironment(1);
    batchTableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

    Table t = getB().groupBy("b").select("b, COLLECT(c) AS d");
    Table result = getA().join(t, "a = b").select("a, d");

    batchTableEnvironment.toDataSet(result, Types.STRING()).print();

    executionEnvironment.execute();
  }

  private static Table getA() throws Exception {
    TableSource src =
      CsvTableSource.builder().path(getFilePath("/data/a.csv")).field("a", Types.STRING()).build();
    return batchTableEnvironment.fromTableSource(src);
  }

  private static Table getB() throws Exception {
    TableSource src =
      CsvTableSource.builder().path(getFilePath("/data/b.csv")).field("b", Types.STRING())
        .field("c", Types.STRING()).build();
    return batchTableEnvironment.fromTableSource(src);
  }

  private static String getFilePath(String relPath) throws UnsupportedEncodingException {
    return URLDecoder.decode(new TableJob1().getClass().getResource(relPath).getFile(),
      StandardCharsets.UTF_8.name());
  }
}
