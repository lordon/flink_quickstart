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

public class TableJob1 {

  private static ExecutionEnvironment executionEnvironment;
  private static BatchTableEnvironment batchTableEnvironment;

  /**
   * Example which demonstrates that a DISTINCT on a JOIN inside of an UNION is not working
   * <p>
   * (
   * SELECT DISTINCT c
   * FROM a JOIN b ON a = b
   * )
   * UNION
   * (
   * SELECT c
   * FROM c
   * )
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    executionEnvironment = ExecutionEnvironment.createLocalEnvironment(1);
    batchTableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

    Table tmpResult = getA().join(getB(), "a=b").select("c").distinct();
    batchTableEnvironment.toDataSet(tmpResult, Types.STRING()).print();

    // Working: tmpResult is not distinct
    /*Table tmpResult = getA().join(getB(), "a=b").select("c");
    batchTableEnvironment.toDataSet(tmpResult, Types.STRING()).print(); */

    Table result = tmpResult.union(getC());
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

  private static Table getC() throws Exception {
    TableSource src =
      CsvTableSource.builder().path(getFilePath("/data/c.csv")).field("d", Types.STRING()).build();
    return batchTableEnvironment.fromTableSource(src);
  }

  private static String getFilePath(String relPath) throws UnsupportedEncodingException {
    return URLDecoder.decode(new TableJob1().getClass().getResource(relPath).getFile(),
      StandardCharsets.UTF_8.name());
  }
}
