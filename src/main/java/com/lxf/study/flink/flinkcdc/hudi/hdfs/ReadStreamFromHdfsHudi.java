package com.lxf.study.flink.flinkcdc.hudi.hdfs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 批量从hudi读数据
 */
public class ReadStreamFromHdfsHudi {
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
                        "  'path' = 'hdfs:///user/hudi/warehouse/ods.db/hudi_t1',\n" +
                        "  'table.type' = 'MERGE_ON_READ',\n" +

                        "  'write.tasks'= '1',\n" +
                        "  'compaction.tasks'= '1',\n" +
                        "  'write.insert.drop.duplicates' = 'true',\n" +

                        "  'read.streaming.enabled' = 'true', \n" +
                        "  'read.start-commit' = '20210316134557', \n" +
                        "  'read.streaming.check-interval' = '5'" +

                        ")");

        Table table = tab.sqlQuery("select id,name,age,create_time,part_dt from hudi_t1");
        tab.toChangelogStream(table).print();


        //启动
        executionEnvironment.execute("ReadBatchFromHdfsHudi");
    }
}
