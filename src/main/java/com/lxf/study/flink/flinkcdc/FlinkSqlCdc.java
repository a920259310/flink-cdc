package com.lxf.study.flink.flinkcdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author xuefeng.lian
 * @Date 2022/4/9 15:46
 * @Version 1.0
 * @DESC
 *
 * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html
 */
public class FlinkSqlCdc {
    public static void main(String[] args) throws Exception {
        //1.创建环境对象
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tab = StreamTableEnvironment.create(executionEnvironment);

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

        Table table = tab.sqlQuery("SELECT * FROM tb_test1");

        tab.toChangelogStream(table).print();

        //启动
        executionEnvironment.execute("FlinkCDC");

    }
}
