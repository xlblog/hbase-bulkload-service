package com.allsenseww.hbase.service.manager;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import com.allsenseww.hbase.service.configuration.OssProperties;
import com.allsenseww.hbase.service.pojo.RecoveryBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.OptionsParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class HBaseManager {

    @Autowired
    private HBaseConnectionManager hBaseConnectionManager;
    @Autowired
    private HBaseProperties hBaseProperties;
    @Autowired
    private OssProperties ossProperties;
    @Autowired
    private HbaseRepairRunner repairRunner;
    /**
     * key:RegionName
     * value:JobId
     */
    private final Map<String, Job> regionBackupMap = new ConcurrentHashMap<>();
    private final Map<String, Job> recoveryMap = new ConcurrentHashMap<>();

    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 创建一个新的表
     */
    public void createNewTable() {
        Connection connection = null;
        try {
            connection = hBaseConnectionManager.getConnection();
            // tsdb
            byte[][] splitKeys = getSplitKeys(hBaseProperties.getBucketCount());
            // rollup 5m
            byte[][] rollupSplitKeys = getSplitKeys(hBaseProperties.getBucketCount(), 8);
            createNewTable(connection, hBaseProperties.getTableName(), splitKeys);
            createNewTable(connection, hBaseProperties.getRepairTableName(), splitKeys);
            createNewTable(connection, hBaseProperties.getRollupTableName(), rollupSplitKeys);
        } finally {
            hBaseConnectionManager.closeConnection(connection);
        }
    }

    /**
     * 合并小的Region
     */
    public void mergeSmallRegion() {
        Connection connection = null;
        Admin admin = null;
        FileSystem fileSystem = null;
        try {
            connection = hBaseConnectionManager.getConnection();
            fileSystem = getFileSystem();
            Map<String, Long> tableRegionMap = getTableRegionSizeMap(fileSystem, hBaseProperties.getTableName());

            admin = connection.getAdmin();
            List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(hBaseProperties.getTableName()));
            byte[] regionA = null;
            long sizeA = 0L;
            for (RegionInfo info : tableRegions) {
                //跨桶的，直接跳过
                byte[] startKey = info.getStartKey();
                byte[] endKey = info.getEndKey();

                if (startKey != null && endKey != null) {
                    byte[] bucketA = null;
                    byte[] bucketB = null;
                    if (startKey.length >= 2) {
                        bucketA = Arrays.copyOf(startKey, 2);
                    }
                    if (endKey.length >= 2) {
                        bucketB = Arrays.copyOf(endKey, 2);
                    }

                    if (bucketB == null || (bucketA != null && !Arrays.equals(bucketA, bucketB))) {
                        regionA = null;
                        continue;
                    }
                }


                String regionName = info.getRegionNameAsString();
                regionName = regionName.substring(regionName.lastIndexOf(".") - 32, regionName.lastIndexOf("."));
                Long size = tableRegionMap.get(regionName);
                size = size == null ? 0 : size;
                // region大小小于upper_size

                int startTime = 0;
                if (startKey != null && startKey.length >= 6) {
                    startTime = Bytes.toInt(Arrays.copyOfRange(startKey, 2, 6));
                }

                if (endKey == null || endKey.length < 6) {
                    break;
                }
                int endTime = Bytes.toInt(Arrays.copyOfRange(endKey, 2, 6));

                long currentTime = System.currentTimeMillis();
                long rangeA = currentTime - hBaseProperties.getShortTimeInterval();
                long rangeB = currentTime - hBaseProperties.getLongTimeInterval();

                // regionA很小的情况下，直接合并
                if (regionA == null && size < hBaseProperties.getLowerSize()) {
                    log.info("regionA size:{}, need merge.", size);
                    regionA = info.getRegionName();
                    sizeA = size;
                    continue;
                }

                if (regionA != null && sizeA < hBaseProperties.getLowerSize()) {
                    //如果A region不为空，则将当前region赋值给B，则直接调用api对A,B进行合并，成功之后清空A,B继续遍历
                    log.info("regionA small, start merge.");
                    byte[] regionB = info.getRegionName();
                    mergeRegion(regionA, regionB, admin);
                    regionA = null;
                    continue;
                }

                if (endTime * 1000L < rangeB || startTime * 1000L > rangeA || size > hBaseProperties.getUpperSize()) {
                    regionA = null;
                } else {
                    if (regionA == null) {
                        //如果A region为空，则将当前region赋值给A
                        regionA = info.getRegionName();
                        sizeA = size;
                    } else {
                        //如果A region不为空，则将当前region赋值给B，则直接调用api对A,B进行合并，成功之后清空A,B继续遍历
                        byte[] regionB = info.getRegionName();
                        mergeRegion(regionA, regionB, admin);
                        regionA = null;
                    }
                }
            }
        } catch (IOException e) {
            log.error("merge hbase small region failed.", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    log.error("close hbase admin failed.", e);
                }
            }
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    log.error("close fileSystem failed.", e);
                }
            }
            hBaseConnectionManager.closeConnection(connection);
        }
    }

    /**
     * 备份HFile到OSS
     */
    public void backupHFileToOSS() {
        String path = df.format(new Date());
        Connection connection = null;
        Admin admin = null;
        FileSystem fileSystem = null;
        try {
            connection = hBaseConnectionManager.getConnection();
            fileSystem = getFileSystem();
            Map<String, Long> tableRegionMap = getTableRegionSizeMap(fileSystem, hBaseProperties.getTableName());

            admin = connection.getAdmin();
            List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(hBaseProperties.getTableName()));
            for (RegionInfo info : tableRegions) {
                String regionName = info.getRegionNameAsString();
                regionName = regionName.substring(regionName.lastIndexOf(".") - 32, regionName.lastIndexOf("."));

                byte[] startKey = info.getStartKey();
                int startBucket = 0;
                if (startKey != null && startKey.length >= 6) {
                    byte[] b = new byte[4];
                    System.arraycopy(startKey, 0, b, 2, 2);
                    startBucket = Bytes.toInt(b);
                }
                byte[] endKey = info.getEndKey();
                if (endKey == null || endKey.length < 6) {
                    continue;
                }
                byte[] b = new byte[4];
                System.arraycopy(endKey, 0, b, 2, 2);
                int endBucket = Bytes.toInt(b);
                int endTime = Bytes.toInt(Arrays.copyOfRange(endKey, 2, 6));
                long currentTime = System.currentTimeMillis();
                long range = currentTime - hBaseProperties.getLongTimeInterval();

                Long fileSize = tableRegionMap.get(regionName);
                if (startBucket == endBucket && endTime * 1000L < range
                        && !regionBackupMap.containsKey(regionName)
                        && fileSize > 10 * 1024L * 1024L) {
                    /*
                    成立条件：
                        1. 桶一样
                        2. 结束时间在backup之前
                        3. 正在进行backup
                        4. 文件大小大于10M（backup之后不会立马进行merge，之后进来的数据不再backup）
                     */
                    log.info("start backup hfile. regionName:{}", regionName);
                    // 需要备份删除
                    String[] args = new String[4];
                    args[0] = "-async";
                    args[1] = "-skipcrccheck";
                    args[2] = "hdfs://" + hBaseProperties.getNameServer() + "/" + getHBaseBasePath(hBaseProperties.getTableName()) + "/" + regionName;
                    args[3] = "oss://" + ossProperties.getAk() + ":" + ossProperties.getSecret() + "@"
                            + ossProperties.getBucket() + "." + ossProperties.getEndpoint() + "/"
                            + ossProperties.getBasePath() + "/" + path + "/";
                    Job job = distcp(args);
                    regionBackupMap.put(regionName, job);
                }
            }
        } catch (Exception e) {
            log.error("backup hfile to oss failed.", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    log.error("close hbase admin failed.", e);
                }
            }
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    log.error("close fileSystem failed.", e);
                }
            }
            hBaseConnectionManager.closeConnection(connection);
        }
    }

    /**
     * 移除已备份的Region
     */
    public void removeBackedRegion() {
        List<String> needRemovedRegions = new ArrayList<>();
        Iterator<Map.Entry<String, Job>> iterator = regionBackupMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Job> entry = iterator.next();
            try {
                Job job = entry.getValue();
                JobStatus.State state = job.getStatus().getState();
                if (JobStatus.State.SUCCEEDED == state) {
                    needRemovedRegions.add(entry.getKey());
                    iterator.remove();
                } else if (JobStatus.State.RUNNING != state) {
                    iterator.remove();
                }
            } catch (Exception e) {
                log.error("get distcp job failed.", e);
            }
        }

        deleteRegions(needRemovedRegions);
    }

    /**
     * 恢复HFiles
     */
    public void recoveryHFiles(RecoveryBean recoveryBean) throws Exception {
        List<String> times = recoveryBean.getTimes();

        if (!recoveryMap.isEmpty()) {
            log.warn("last recovery can not finish.");
            return;
        }

        // 恢复数据到对应的表
        for (String time : times) {
            log.info("start recovery hfiles. time:{}", time);
            // 需要备份删除
            String[] args = new String[4];
            args[0] = "-async";
            args[1] = "-skipcrccheck";
            args[3] = "hdfs://" + hBaseProperties.getNameServer() + "/tmp/recovery/" + hBaseProperties.getRepairTableName() + "/" + time;
            args[2] = "oss://" + ossProperties.getAk() + ":" + ossProperties.getSecret() + "@"
                    + ossProperties.getBucket() + "." + ossProperties.getEndpoint() + "/"
                    + ossProperties.getBasePath() + "/" + time + "/*";
            Job job = distcp(args);
            recoveryMap.put(time, job);
        }
    }

    public void recoveryTable() throws Exception {
        if (!recoveryMap.isEmpty()) {
            Collection<Job> jobs = recoveryMap.values();
            // 检查到其中有一个未完成备份，则跳过
            for (Job job : jobs) {
                JobStatus.State state = job.getStatus().getState();
                if (JobStatus.State.RUNNING == state || JobStatus.State.PREP == state) {
                    return;
                }
            }

            Set<String> times = recoveryMap.keySet();
            log.info("times size:{}", times.size());
            // 修复meta表
            for (String time : times) {
                String path = "hdfs://" + hBaseProperties.getNameServer() + "/tmp/recovery/" + hBaseProperties.getRepairTableName() + "/" + time;
                log.info("start recover meta info. path:{}", path);
                repairRunner.createRegionInfo(path);
            }
            // 全部完成以后
            recoveryMap.clear();
        }
    }

    private FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
        configuration.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        configuration.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return FileSystem.get(configuration);
    }

    private byte[][] getSplitKeys(Integer regionCount) {
        byte[][] splitKeys = new byte[regionCount - 1][];
        for (int i = 1; i < regionCount; i++) {
            byte[] bytes = Bytes.toBytes(i);
            splitKeys[i - 1] = Arrays.copyOfRange(bytes, 2, 4);
        }
        return splitKeys;
    }

    private byte[][] getSplitKeys(Integer regionCount, Integer step) {
        int count = (regionCount / step) - 1;
        byte[][] splitKeys = new byte[count][];
        for (int i = 1; i < count + 1; i++) {
            byte[] bytes = Bytes.toBytes(i * step);
            splitKeys[i - 1] = Arrays.copyOfRange(bytes, 2, 4);
        }
        return splitKeys;
    }

    private void createNewTable(Connection connection, String newTableName, byte[][] splitKeys) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("default:" + newTableName);
            if (admin.tableExists(tableName)) {
                return;
            }
            //定义列簇
            ColumnFamilyDescriptor columnFamilyDes = ColumnFamilyDescriptorBuilder.newBuilder("t".getBytes())
                    .setMaxVersions(1)
                    .setCompressionType(Compression.Algorithm.SNAPPY)
                    .setDataBlockEncoding(DataBlockEncoding.DIFF)
                    .setBloomFilterType(BloomType.ROW)
                    .setInMemoryCompaction(MemoryCompactionPolicy.BASIC)
                    .build();

            TableDescriptor tableDes = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(columnFamilyDes)
                    .build();
            admin.createTable(tableDes, splitKeys);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    log.error("create table admin close failed.", e);
                }
            }
        }
    }


    /**
     * 获得table的region名称和size对应关系的map
     */
    private Map<String, Long> getTableRegionSizeMap(FileSystem fs, String tableName) throws IOException {
        Map<String, Long> tableRegionSizeMap = new HashMap<>();
        String hbase_path = getHBaseBasePath(tableName);

        Path table_path = new Path(hbase_path);
        FileStatus[] files = fs.listStatus(table_path);

        for (FileStatus file : files) {
            String name = file.getPath().getName();
            long length = fs.getContentSummary(file.getPath()).getLength();
            tableRegionSizeMap.put(name, length);
        }

        return tableRegionSizeMap;
    }

    private String getHBaseBasePath(String tableName) {
        String hbase_path = "/hbase/data/";

        if (tableName.contains(":")) {
            String[] split = tableName.split(":");
            hbase_path = hbase_path + split[0] + "/" + split[1];
        } else {
            hbase_path = hbase_path + "default/" + tableName;
        }
        return hbase_path;
    }

    private Job distcp(String[] argv) throws Exception {
        if (argv.length < 1) {
            OptionsParser.usage();
            return null;
        } else {
            DistCpOptions distCpOptions = OptionsParser.parse(argv);
            Configuration configuration = new Configuration();
            DistCp distCp = new DistCp(configuration, distCpOptions);
            return distCp.execute();
        }
    }

    private void mergeRegion(byte[] regionA, byte[] regionB, Admin admin) throws IOException {
        long currentTimeMillis = System.currentTimeMillis();
        byte[][] nameofRegionsToMerge = new byte[2][];
        nameofRegionsToMerge[0] = regionA;
        nameofRegionsToMerge[1] = regionB;
        admin.mergeRegionsAsync(nameofRegionsToMerge, false);
        long currentTimeMillis2 = System.currentTimeMillis();
        log.info(Bytes.toString(regionA) + " & " + Bytes.toString(regionB)
                + "merge cost " + (currentTimeMillis2 - currentTimeMillis) + " milliseconds!");
    }

    /**
     * 删除已经备份的Regions
     */
    public void deleteRegions(List<String> regionNames) {
        Connection connection = null;
        Admin admin = null;
        FileSystem fileSystem = null;
        try {
            connection = hBaseConnectionManager.getConnection();
            fileSystem = getFileSystem();
            //删除hfile
            for (String regionName : regionNames) {
                log.info("start delete region. regionName:{}", regionName);
                fileSystem.delete(new Path(getHBaseBasePath(hBaseProperties.getTableName() + "/" + regionName)), true);
            }
            admin = connection.getAdmin();

            List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(hBaseProperties.getTableName()));

            byte[] regionA = null;
            for (RegionInfo info : tableRegions) {
                String regionName = info.getRegionNameAsString();
                regionName = regionName.substring(regionName.lastIndexOf(".") - 32, regionName.lastIndexOf("."));

                if (regionA != null) {
                    byte[][] nameofRegionsToMerge = new byte[2][];
                    nameofRegionsToMerge[0] = regionA;
                    nameofRegionsToMerge[1] = info.getRegionName();
                    admin.mergeRegionsAsync(nameofRegionsToMerge, false);
                    regionA = null;
                } else if (regionNames.contains(regionName)) {
                    regionA = info.getRegionName();
                }

            }
        } catch (Exception e) {
            log.error("remove region failed.", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    log.error("close hbase admin failed.", e);
                }
            }
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    log.error("close fileSystem failed.", e);
                }
            }
            hBaseConnectionManager.closeConnection(connection);
        }
    }

}
