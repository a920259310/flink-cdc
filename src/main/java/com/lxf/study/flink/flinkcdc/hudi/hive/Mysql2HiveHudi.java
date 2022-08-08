package com.lxf.study.flink.flinkcdc.hudi.hive;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * mysql数据写入到Hudi
 */
public class Mysql2HiveHudi {
    public static void main(String[] args) throws Exception {

//        System.setProperty("HADOOP_USER_NAME","uintegrate");

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
                        " create_time TIMESTAMP(3)\n" +
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
                "CREATE TABLE hudi_hive_t1(\n" +
                        "  id INT PRIMARY KEY NOT ENFORCED,\n" +
                        "  name STRING,\n" +
                        "  age INT,\n" +
                        "  create_time TIMESTAMP(3),\n" +
                        "  part_dt STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (part_dt)\n" +
                        "WITH (\n" +
                        "  'connector' = 'hudi',\n" +
                        "  'path' = 'hdfs:///user/hudi/warehouse/ods.db/hudi_hive_t1',\n" +
//                        "  'table.type' = 'MERGE_ON_READ',\n" +
                        "  'table.type' = 'COPY_ON_WRITE',\n" +

                        "   'write.precombine.field'= 'create_time',\n" +
                        "   'write.insert.drop.duplicates' = 'true',\n" +
                        "   'write.tasks'= '1',\n" +
                        "   'write.rate.limit'= '2000', \n" +

                        "   'compaction.tasks'= '1', \n" +
                        "   'compaction.async.enabled'= 'true',\n" +
                        "   'compaction.trigger.strategy'= 'num_commits',\n" +
                        "   'compaction.delta_commits'= '1',\n" +

//                        "   'changelog.enabled'= 'true',\n" +
                        "   'read.streaming.enabled'= 'true',\n" +
                        "   'read.streaming.check-interval'= '5'," +

                        "  'hive_sync.enable' = 'true',\n" +
                        "  'hive_sync.mode' = 'hms',\n" +
                        "  'hive_sync.metastore.uris' = 'thrift://dev-bigdata-04:9083',\n" +
                        "  'hive_sync.table'='hudi_hive_t1',\n" +
                        "  'hive_sync.db'='t_ods'," +

                        "  'hive_sync.support_timestamp'= 'true'" +

                        ")");

        tab.executeSql("insert into hudi_hive_t1 select id,name,age,create_time,date_format(create_time,'yyyy-MM-dd') as part_dt from tb_test1");

        Table table = tab.sqlQuery("select * from hudi_hive_t1");
        tab.toChangelogStream(table).print();

        //启动
        executionEnvironment.execute("Mysql2Hudi");
    }
}
