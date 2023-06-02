package com.lqf.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HudiTable {

    public static void main(String[] args) {


        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

//        bsEnv.setStateBackend(new FsStateBackend("file:///Users/liao/Downloads/learn/hudi/ck"));
        bsEnv.setStateBackend(new MemoryStateBackend());
//        // 每 1000ms 开始一次 checkpoint
        bsEnv.enableCheckpointing(10000);
//
//        // 高级选项：
//
//        设置模式为精确一次 (这是默认值)
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        确认 checkpoints 之间的时间会进行 500 ms
//        bsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//
//        Checkpoint 必须在一分钟内完成，否则就会被抛弃
//        bsEnv.getCheckpointConfig().setCheckpointTimeout(60000);
//
//        同一时间只允许一个 checkpoint 进行
//        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        开启在 job 中止后仍然保留的 externalized checkpoints
//        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        bsEnv.setParallelism(1);


        StreamTableEnvironment env = StreamTableEnvironment.create(bsEnv, bsSettings);

        env.executeSql("create table t2(\n" +
                "  uuid VARCHAR(20), \n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  `partition` INT\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'fields.partition.max' = '5',\n" +
                "    'fields.partition.min' = '1',\n" +
                "    'rows-per-second' = '1'\n" +
                ")");
//        env.executeSql("select * from t2").print();
//        env.executeSql("CREATE TABLE t1(uuid VARCHAR(20),name VARCHAR(10),age INT,ts TIMESTAMP(3),`partition` INT) PARTITIONED BY (`partition`) WITH ('connector' ='hudi','path' = 'file:///home/monica/study/flink/flink-parent/flink-hudi/src/main/resources/test','write.tasks' = '1', 'compaction.tasks' = '1', 'table.type' = 'COPY_ON_WRITE')");


        env.executeSql("CREATE TABLE t1(\n" +
                "  uuid VARCHAR(20), -- you can use 'PRIMARY KEY NOT ENFORCED' syntax to mark the field as record key\n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  `partition` INT\n" +
                ")\n" +
                "PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'file:///Users/liao/Downloads/learn/hudi/ck',\n" +
                "  'write.tasks' = '1', -- default is 4 ,required more resource\n" +
                "  'read.streaming.enabled'= 'true',  \n" +
                "  'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE\n" +
                ")");

        //插入一条数据
//        env.executeSql("INSERT INTO t1 SELECT * FROM source_table");
//        env.sqlQuery("SELECT * FROM t1")//结果①
//                .execute()
//                .print();

//        //修改数据
        env.executeSql("INSERT INTO t1 select * from t2")
                .print();
//        env.executeSql("INSERT INTO t1 VALUES('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01','par1')")
//                .print();
//        env.sqlQuery("SELECT * FROM t1")//结果②
//                .execute()
//                .print();



    }
}
