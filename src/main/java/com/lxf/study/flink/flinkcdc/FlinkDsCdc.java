package com.lxf.study.flink.flinkcdc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
/**
 * @Author xuefeng.lian
 * @Date 2022/4/9 13:39
 * @Version 1.0
 * @DESC
 */
public class FlinkDsCdc {
    public static void main(String[] args) throws Exception {

        //1.创建环境信息
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "D:\\study\\code\\flink-cdc\\ck\\6f10b05dc48fef025d0a4ac5cc080921\\chk-15");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //2.开启ck
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new FsStateBackend("file:///D:\\study\\code\\flink-cdc\\ck"));


        //3.创建数据源连接
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("testdb") // set captured database
                .tableList("testdb.t1") // set captured table
                .startupOptions(StartupOptions.initial())
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
