package com.allsenseww.hbase.service.controller;

import com.allsenseww.hbase.service.configuration.HBaseProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.OptionsParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class TestController {

    @Autowired
    private HBaseProperties hBaseProperties;

    @GetMapping("/test")
    @ResponseBody
    public String test() {
        return "ok";
    }

    @GetMapping("/test/distcp")
    @ResponseBody
    public String testDistcp() throws Exception {
        Configuration configuration = new Configuration();
        String[] args = new String[4];
        args[0] = "-async";
        args[1] = "-skipcrccheck";
        args[2] = "oss://LTAI5tKKVhCxdqjys4qP4jEX:qXe8VzWqB4GVqy7aeEwxADou9wChG9@hdfs-archive.oss-cn-zhangjiakou.aliyuncs.com/hbase//085499ad7d0b820dc21e6fc0f5293f3a/085499ad7d0b820dc21e6fc0f5293f3a/t/ae516aebb9164dd39d02ea51a86bddcf";
        args[3] = "hdfs:///tmp/distcp/lizy";
        DistCpOptions distCpOptions = OptionsParser.parse(args);
        DistCp distCp = new DistCp(configuration, distCpOptions);
        Job job = distCp.execute();
        System.out.println("job submit success");

        JobStatus.State state = job.getStatus().getState();
        String id = job.getJobID().toString();

        System.out.println("state:" + state.name() + " id:" + id);
        return "ok";
    }

    @GetMapping("/test/load")
    @ResponseBody
    public String testLoad() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hBaseProperties.getZkQuorum());
        conf.set("hbase.zookeeper.property.clientPort", hBaseProperties.getZkPort());
        LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(conf);
        String[] args = new String[]{"/tmp/allsmart/bulkload/bucket_tsdb/1633536000000", "bucket_tsdb"};
        loadIncrementalHFiles.run(args);
        return "ok";
    }

}
