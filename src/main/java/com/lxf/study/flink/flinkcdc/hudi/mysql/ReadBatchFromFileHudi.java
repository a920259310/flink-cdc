package com.lxf.study.flink.flinkcdc.hudi.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 批量从hudi读数据
 */
public class ReadBatchFromFileHudi {
    public static void main(String[] args) throws Exception {
        //1.创建环境对象
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        StreamTableEnvironment tab = StreamTableEnvironment.create(executionEnvironment);
        //1.创建Hudi映射表
        tab.executeSql(
                "CREATE TABLE hudi_t1(\n" +
                        "  id INT PRIMARY KEY NOT ENFORCED,\n" +
                        "  name STRING,\n" +
                        "  age INT,\n" +
                        "  create_time STRING,\n" +
                        "  part_dt STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (part_dt)\n" +
                        "WITH (\n" +
                        "  'connector' = 'hudi',\n" +
                        "  'path' = 'file:///D:\\study\\code\\flink-cdc\\hudi\\hudi_t1',\n" +
                        "  'table.type' = 'MERGE_ON_READ'\n" +
                        ")");

        Table table = tab.sqlQuery("select id,name,age,create_time,part_dt from hudi_t1");
        tab.toChangelogStream(table).print();


        //启动
        executionEnvironment.execute("BatchReadHudi");
    }
}
