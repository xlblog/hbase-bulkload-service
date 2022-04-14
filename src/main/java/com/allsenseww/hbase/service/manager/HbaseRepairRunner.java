package com.allsenseww.hbase.service.manager;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hbase.regionserver.HRegionFileSystem.REGION_INFO_FILE;

@Slf4j
@Component
public class HbaseRepairRunner {

    @Autowired
    private HBaseProperties hBaseProperties;


    final String TABLE = "hbase:meta";
    final String FAMILY = "info";
    final String SN = "sn";
    final String SERVER = "server";
    final String STATE = "state";

    public void createRegionInfo(String pathAdd) throws Exception {
        String HBASE_QUORUM = "hbase.zookeeper.quorum";
        String HBASE_ROOTDIR = "hbase.rootdir";
        String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_QUORUM, hBaseProperties.getZkQuorum());
        conf.set(HBASE_ROOTDIR, hBaseProperties.getHdfsRootDir());
        conf.set(HBASE_ZNODE_PARENT, "/hbase");

        // 获取备份表的Region信息
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        List<RegionInfo> repairTableRegions = admin.getRegions(TableName.valueOf(hBaseProperties.getRepairTableName()));
        Map<String, String> bucketMap = new HashMap<>();
        for (RegionInfo regionInfo : repairTableRegions) {
            byte[] startKey = regionInfo.getStartKey();
            int bucket = 0;
            if (startKey != null && startKey.length > 0) {
                byte[] bucketByte = new byte[]{0, 0, 0, 0};
                System.arraycopy(startKey, 0, bucketByte, 2, 2);
                bucket = Bytes.toInt(bucketByte);
            }
            String regionName = regionInfo.getRegionNameAsString();
            regionName = regionName.substring(regionName.lastIndexOf(".") - 32, regionName.lastIndexOf("."));
            bucketMap.put(String.valueOf(bucket), regionName);
        }

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathAdd);

        FileStatus[] list = fs.listStatus(path);
        for (FileStatus status : list) {
            if (!status.isDirectory()) {
                continue;
            }

            boolean isRegion = false;
            FileStatus[] regions = fs.listStatus(status.getPath());
            FileStatus[] hfiles = null;
            for (FileStatus regionStatus : regions) {
                if (regionStatus.toString().contains(REGION_INFO_FILE)) {
                    isRegion = true;
                    continue;
                }
                if (!regionStatus.toString().endsWith("t")) {
                    hfiles = fs.listStatus(regionStatus.getPath());
                }
            }

            if (!isRegion) {
                continue;
            }

            RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, status.getPath());

            byte[] startKey = hri.getStartKey();
            int bucket = 0;
            if (startKey != null && startKey.length > 0) {
                byte[] bucketByte = new byte[]{0, 0, 0, 0};
                System.arraycopy(startKey, 0, bucketByte, 2, 2);
                bucket = Bytes.toInt(bucketByte);
            }

            String tablePath = "hdfs://" + hBaseProperties.getHdfsRootDir() + "/data/default/" + hBaseProperties.getRepairTableName() + "/";
            String repairRegionName = bucketMap.get(String.valueOf(bucket));
            if (hfiles != null) {
                for (FileStatus hfile : hfiles) {
                    log.info("move src:{} to dist:{}", hfile.getPath().toString(), tablePath + repairRegionName);
                    FileUtil.copy(fs, hfile.getPath(), fs, new Path(tablePath + repairRegionName + "/t/" + hfile.getPath().getName()), true, conf);
                }
            }
        }

        admin.disableTable(TableName.valueOf(hBaseProperties.getRepairTableName()));
        admin.enableTable(TableName.valueOf(hBaseProperties.getRepairTableName()));

        admin.close();
        fs.close();
        conn.close();
    }


}

