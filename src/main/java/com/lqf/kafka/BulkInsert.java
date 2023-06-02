package com.lqf.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BulkInsert {

    public static void main(String[] args) {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode();

//        bsEnv.setStateBackend(new FsStateBackend("file:///Users/liao/Downloads/learn/hudi/ck"));
//        bsEnv.setStateBackend(new MemoryStateBackend());
//        // 每 1000ms 开始一次 checkpoint
//        bsEnv.enableCheckpointing(10000);
//
//        // 高级选项：
//
//        设置模式为精确一次 (这是默认值)
//        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
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


//        StreamTableEnvironment env = StreamTableEnvironment.create(bsEnv, bsSettings);
        TableEnvironment env = TableEnvironment.create(builder.build());



        env.executeSql("create table test1(\n" +
                "session_id string,\n" +
                "from_cluster string,\n" +
                "general_message string\n" +
                "\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='first',\n" +
                "'properties.bootstrap.servers' = 'linux01:9092',\n" +
                "'properties.group.id' = 'testGroup',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format'='json'\n" +
                "\n" +
                ")");
//        env.executeSql("select * from t2").print();
//        env.executeSql("CREATE TABLE t1(uuid VARCHAR(20),name VARCHAR(10),age INT,ts TIMESTAMP(3),`partition` INT) PARTITIONED BY (`partition`) WITH ('connector' ='hudi','path' = 'file:///home/monica/study/flink/flink-parent/flink-hudi/src/main/resources/test','write.tasks' = '1', 'compaction.tasks' = '1', 'table.type' = 'COPY_ON_WRITE')");


        env.executeSql("create table bulk1(\n" +
                "session_id string,\n" +
                "from_cluster string,\n" +
                "general_message string\n" +
                ")\n" +
                "with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'file:///Users/liao/Downloads/learn/hudi/storage/bulk',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'write.operation' = 'bulk_insert',\n" +
                "'write.tasks'='1',\n" +
                "'compaction.tasks'='1',\n" +
                "'read.tasks'='4'\n" +
                ")");

        //插入一条数据
//        env.executeSql("INSERT INTO t1 SELECT * FROM source_table");
//        env.sqlQuery("SELECT * FROM t1")//结果①
//                .execute()
//                .print();

//        //修改数据
        env.executeSql("INSERT INTO bulk1 select * from test1")
                .print();
    }
}
