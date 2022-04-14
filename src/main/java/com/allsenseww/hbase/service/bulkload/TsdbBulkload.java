package com.allsenseww.hbase.service.bulkload;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import com.allsenseww.hbase.service.pojo.BulkloadBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormatBase;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.*;
import java.util.function.Consumer;

@Component
@Slf4j
public class TsdbBulkload implements Serializable {

    @Autowired
    private HBaseProperties hBaseProperties;

    public BulkloadResult bulkload(BulkloadBean bulkloadBean) throws Exception {
        String flinkDistJar = "./lib/flink-dist_2.11-1.10.1.jar";
        String zkQuorum = hBaseProperties.getZkQuorum();
        String sourceTableName = bulkloadBean.getSourceTableName();
        String targetTableName = bulkloadBean.getTargetTableName() == null ? hBaseProperties.getTableName() : bulkloadBean.getTargetTableName();
        Long startTime = bulkloadBean.getStartTime();
        Long endTime = bulkloadBean.getEndTime();
        Integer bucket = hBaseProperties.getBucketCount();

        List<String> userJars = new ArrayList<>();
        addJars("./lib", userJars);

        String jobId = "bulkload-" + UUID.randomUUID().toString();

        // flink env
        ExecutionEnvironment environment = new FlinkEnvironmentBuild()
                .setFlinkDistJar(flinkDistJar)
                .setUserJars(userJars)
                .setTaskId(jobId)
                .build();

        environment.setParallelism(bulkloadBean.getParallelism());
        boolean rollup = bulkloadBean.getRollup();
//        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        String outputPath = "/tmp/allsmart/bulkload/" + targetTableName + "/" + startTime;

        HadoopOutputFormatBase outputFormat;
        if (rollup) {
            outputFormat = HFileOutputUtil.buildRollupHadoopOutputFormat(zkQuorum, targetTableName, outputPath);
        } else if (targetTableName.contains("rollup")) {
            outputFormat = HFileOutputUtil.buildHadoopOutputFormat3(zkQuorum, targetTableName, outputPath);
        } else {
            outputFormat = HFileOutputUtil.buildHadoopOutputFormat(zkQuorum, targetTableName, outputPath);
        }


        // hbase input format
        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(HBaseOptions.ZOOKEEPER_QUORUM_OPTION.key(), zkQuorum);
        hbaseConfig.set(HBaseOptions.ZOOKEEPER_CLIENT_PORT_OPTION.key(), "2181");
        hbaseConfig.setLong("hbase.rpc.timeout", 1200000);
        HBaseInputFormat hBaseInputFormat = new HBaseInputFormat(sourceTableName, startTime, endTime, hbaseConfig);
        DataSource<HBaseResult> dataSource = environment.createInput(hBaseInputFormat);


        dataSource.map(new MapFunction<HBaseResult, Tuple2<String, HBaseResult>>() {
            @Override
            public Tuple2<String, HBaseResult> map(HBaseResult value) throws Exception {
                byte[] row_key = value.getRow();

                // tags 开始位置
                final int tags_start = 3 + 4;

                // 计算桶
                final byte[] salt_base = new byte[row_key.length - 4];
                System.arraycopy(row_key, 0, salt_base, 0, 3);
                System.arraycopy(row_key, tags_start, salt_base, 3,
                        row_key.length - tags_start);
                int modulo = Arrays.hashCode(salt_base) % bucket;
                if (modulo < 0) {
                    // make sure we return a positive salt.
                    modulo = modulo * -1;
                }

                // 获取时间
                byte[] time = new byte[4];
                System.arraycopy(row_key, 3, time, 0, 4);

                // 计算盐
                final byte[] salt = getSaltBytes(modulo, time);

                // 得到新的rowkey
                byte[] new_row_key = new byte[6 + row_key.length];
                System.arraycopy(salt, 0, new_row_key, 0, 6);
                System.arraycopy(row_key, 0, new_row_key, 6, row_key.length);

                value.setRow(new_row_key);
                return new Tuple2<>(Bytes.toHex(value.getRow()), value);
            }
        }).sortPartition(0, Order.ASCENDING).map(new MapFunction<Tuple2<String, HBaseResult>, HBaseResult>() {
            @Override
            public HBaseResult map(Tuple2<String, HBaseResult> value) throws Exception {
                return value.f1;
            }
        }).flatMap(new FlatMapFunction<HBaseResult, Tuple2<ImmutableBytesWritable, Cell>>() {
            @Override
            public void flatMap(HBaseResult value, Collector<Tuple2<ImmutableBytesWritable, Cell>> out) throws Exception {
                byte[] row = value.getRow();
                ImmutableBytesWritable rowkeyWritable = new ImmutableBytesWritable(row);
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = value.getMap();

                byte[] familyByte = new byte[0];
                List<Tuple2<String, byte[]>> qvs = new ArrayList<>();
                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                    familyByte = entry.getKey();

                    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifyMap = entry.getValue();
                    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifyEntry : qualifyMap.entrySet()) {
                        byte[] qualifyByte = qualifyEntry.getKey();
                        Object[] values = qualifyEntry.getValue().values().toArray();
                        byte[] v = (byte[]) values[values.length - 1];
                        qvs.add(new Tuple2<>(Bytes.toHex(qualifyByte), v));
                    }
                }

                byte[] finalFamilyByte = familyByte;
                qvs.stream().sorted(new Comparator<Tuple2<String, byte[]>>() {
                    @Override
                    public int compare(Tuple2<String, byte[]> o1, Tuple2<String, byte[]> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                }).forEach(new Consumer<Tuple2<String, byte[]>>() {
                    @Override
                    public void accept(Tuple2<String, byte[]> tuple2) {
                        KeyValue keyValue = new KeyValue(row, finalFamilyByte, Bytes.fromHex(tuple2.f0), tuple2.f1);
                        out.collect(new Tuple2<>(rowkeyWritable, keyValue));
                    }
                });
            }
        }).output(outputFormat);

        environment.execute(jobId);
        log.info("submit bulkload job. start:{}, end:{}, jobId:{}", startTime, endTime, jobId);
        BulkloadResult result = new BulkloadResult();
        result.setJobId(jobId);
        result.setOutputPath(outputPath);
        result.setTargetTable(bulkloadBean.getTargetTableName());
        result.setAutoLoad(!rollup);
        return result;
    }

    public byte[] getSaltBytes(final int bucket, final byte[] time) {
        byte[] target = new byte[6];
        byte[] bytes = Bytes.toBytes(bucket);
        System.arraycopy(bytes, 2, target, 0, 2);
        System.arraycopy(time, 0, target, 2, 4);
        return target;
    }

    private void addJars(String path, List<String> jars) {
        if (path != null && !path.isEmpty()) {
            File file = new File(path);
            if (file.exists() && file.isDirectory()) {
                File[] jarFiles = file.listFiles();
                if (jarFiles == null) {
                    return;
                }
                for (File jarFile : jarFiles) {
                    if (jarFile.getName().toUpperCase().endsWith(".JAR")) {
                        try {
                            jars.add(jarFile.toURI().toURL().toString());
                        } catch (MalformedURLException e) {
                            log.error("add jarFile error.", e);
                        }
                    }
                }
            }
        }
    }

}
