package com.lxf.study.flink.flinkcdc.hudi.hdfs;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * mysql数据写入到Hudi
 */
public class Mysql2HdfsHudi {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","uintegrate");

        //1.创建环境对象
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        StreamTableEnvironment tab = StreamTableEnvironment.create(executionEnvironment);

        executionEnvironment.setStateBackend(new FsStateBackend("file:///D:\\study\\code\\flink-cdc\\ck"));
        executionEnvironment.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = executionEnvironment.getCheckpointConfig();
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        //1.创建mysql映射表
        tab.executeSql(
                "CREATE TABLE tb_test1 (\n" +
                        " id INT primary key,\n" +
                        " name STRING,\n" +
                        " age INT,\n" +
                        " create_time timestamp\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'scan.startup.mode' = 'initial',\n" +
                        " 'hostname' = 'localhost',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'root',\n" +
                        " 'database-name' = 'test',\n" +
                        " 'table-name' = 'tb_test1'\n" +
                        ")");
//        tab.toChangelogStream(tab.sqlQuery("select id,name,age,date_format(create_time,'yyyy-MM-dd HH:mm:ss') as create_time,date_format(create_time,'yyyy-MM-dd') as part_dt from tb_test1")).print();


        //2.创建Hudi映射表
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
                        "  'write.insert.drop.duplicates' = 'true'\n" +

                        ")");

        tab.executeSql("insert into hudi_t1 select id,name,age,date_format(create_time,'yyyy-MM-dd HH:mm:ss') as create_time,date_format(create_time,'yyyy-MM-dd') as part_dt from tb_test1");


        //启动
//        executionEnvironment.execute("Mysql2Hudi");
    }
}
