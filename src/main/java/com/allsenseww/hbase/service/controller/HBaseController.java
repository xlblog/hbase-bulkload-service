package com.allsenseww.hbase.service.controller;

import com.allsenseww.hbase.service.bulkload.BulkloadResult;
import com.allsenseww.hbase.service.bulkload.TsdbBulkload;
import com.allsenseww.hbase.service.configuration.HBaseProperties;
import com.allsenseww.hbase.service.manager.HBaseManager;
import com.allsenseww.hbase.service.pojo.BulkloadBatchBean;
import com.allsenseww.hbase.service.pojo.BulkloadBean;
import com.allsenseww.hbase.service.pojo.HfileLoadBean;
import com.allsenseww.hbase.service.pojo.RecoveryBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Controller
@Slf4j
public class HBaseController {

    @Autowired
    private HBaseManager hBaseManager;
    @Autowired
    private HBaseProperties hBaseProperties;
    @Autowired
    private TsdbBulkload tsdbBulkload;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private BulkloadResult lastBulkloadResult;
    private final Queue<BulkloadBean> queue = new ConcurrentLinkedQueue<>();

    @PostConstruct
    public void init() {
        try {
            hBaseManager.createNewTable();
        } catch (Exception e) {
            log.error("run tsdb bulkload failed.", e);
        }

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            YarnClient yarnClient = null;
            try {
                if (queue.isEmpty()) {
                    return;
                }
                if (lastBulkloadResult == null) {
                    //第一次提交
                    submitJob();
                } else {
                    // 查看状态，finish之后再提交下一个
                    yarnClient = initYarnClient();
                    ApplicationReport applicationReport = getApplicationReport(yarnClient, lastBulkloadResult.getJobId());

                    if (applicationReport != null) {
                        YarnApplicationState yarnApplicationState = applicationReport.getYarnApplicationState();
                        FinalApplicationStatus finalApplicationStatus = applicationReport.getFinalApplicationStatus();

                        if (yarnApplicationState == YarnApplicationState.FINISHED || yarnApplicationState == YarnApplicationState.FAILED) {
                            // 完成
                            if (finalApplicationStatus == FinalApplicationStatus.FAILED || finalApplicationStatus == FinalApplicationStatus.KILLED) {
                                log.warn("job submit failed. jobId:{}, startTime:{}, endTime:{}", lastBulkloadResult.getJobId(),
                                        lastBulkloadResult.getStartTime(), lastBulkloadResult.getEndTime());
                                lastBulkloadResult = null;
                            } else {
                                lastBulkloadResult = null;
                            }

                            submitJob();
                        }

                    } else {
                        submitJob();
                    }

                }
            } catch (Exception ex) {
                log.error("get yarn application state failed.", ex);
            } finally {
                try {
                    closeYarnClient(yarnClient);
                } catch (IOException e) {
                    log.error("close yarn client failed.", e);
                }
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    private void submitJob() {
        BulkloadBean bulkloadBean = queue.poll();
        if (bulkloadBean == null) {
            return;
        }
        log.info("start submit queue job. queue remaining {} jobs.", queue.size());
        try {
            lastBulkloadResult = tsdbBulkload.bulkload(bulkloadBean);
        } catch (Exception e) {
            log.error("submit bulkload job failed.", e);
        }
    }

    public void batchBulkload(List<BulkloadBean> bulkloadBeans) {
        queue.addAll(bulkloadBeans);
    }

    @Scheduled(cron = "${app.merge.region.cron:0 0 0/6 * * ?}")
    public void mergeSmallRegion() {
        log.info("merge small region check.");
        hBaseManager.mergeSmallRegion();
        log.info("finish merge small region.");
    }

    @Scheduled(cron = "${app.backup.hfile.cron:0 20 2 1/1 * ?}")
    public void backupHFile() {
        log.info("backup hfile to oss check.");
        hBaseManager.backupHFileToOSS();
        log.info("backup hfile to oss finish.");
    }

    @Scheduled(cron = "${app.remove.region.cron:0 0 0/1 * * ?}")
    public void removeBackedRegion() {
        log.info("remove backed region check.");
        hBaseManager.removeBackedRegion();
        log.info("remove backed region finish.");
    }

    @Scheduled(cron = "${app.recovery.region.cron:0 0/5 * * * ?}")
    public void recoveryTable() {
        try {
            hBaseManager.recoveryTable();
        } catch (Exception e) {
            log.error("recovery table failed", e);
        }
    }

    @PostMapping("/recovery/hfiles")
    @ResponseBody
    public String recovery(@RequestBody RecoveryBean recoveryBean) {
        if (recoveryBean.getTimes() != null && recoveryBean.getTimes().size() > 0) {
            try {
                hBaseManager.recoveryHFiles(recoveryBean);
            } catch (Exception e) {
                log.error("recovery failed.", e);
            }
        }
        return "ok";
    }

    @GetMapping("/recovery/table")
    @ResponseBody
    public String recoveryTableRest() {
        try {
            hBaseManager.recoveryTable();
        } catch (Exception e) {
            log.error("recovery table failed", e);
        }
        return "ok";
    }

    @PostMapping("/tsdb/bulkload")
    @ResponseBody
    public String bulkload(@RequestBody BulkloadBean bulkloadBean) {
        try {
            tsdbBulkload.bulkload(bulkloadBean);
        } catch (Exception e) {
            log.error("bulkload failed.", e);
        }
        return "ok";
    }

    @PostMapping("/tsdb/batch/bulkload")
    @ResponseBody
    public String bulkload(@RequestBody List<BulkloadBean> bulkloadBeans) {
        if (queue.isEmpty()) {
            try {
                log.info("batch bulkload has submit.");
                batchBulkload(bulkloadBeans);
            } catch (Exception e) {
                log.error("bulkload failed.", e);
            }
        } else {
            log.info("last batch bulkload can not finish.");
        }
        return "ok";
    }

    @PostMapping("/tsdb/batch/bulkload2")
    @ResponseBody
    public String batchBulkload(@RequestBody BulkloadBatchBean batchBean) {
        if (batchBean.getStartTime() == null || batchBean.getEndTime() == null || batchBean.getTimeInterval() == null || batchBean.getStartTime() > batchBean.getEndTime()) {
            return "parameters is null";
        }
        Long endTime = batchBean.getEndTime();
        List<BulkloadBean> bulkloadBeans = new ArrayList<>();
        while (endTime.compareTo(batchBean.getStartTime()) > 0) {
            BulkloadBean bulkloadBean = new BulkloadBean();
            bulkloadBean.setSourceTableName(batchBean.getSourceTableName());
            bulkloadBean.setTargetTableName(batchBean.getTargetTableName());
            bulkloadBean.setStartTime(endTime - batchBean.getTimeInterval());
            bulkloadBean.setEndTime(endTime);
            bulkloadBean.setRollup(batchBean.getRollup());
            bulkloadBean.setParallelism(batchBean.getParallelism());
            bulkloadBeans.add(bulkloadBean);
            endTime -= batchBean.getTimeInterval();
        }
        log.info("total jobs {}", bulkloadBeans.size());
        batchBulkload(bulkloadBeans);
        return "ok";
    }

    @ResponseBody
    @PostMapping("/tsdb/load")
    public String loadHfile(@RequestBody HfileLoadBean loadBean) {
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                for (String outputPath : loadBean.getOutputPaths()) {
                    log.info("start load hfile to hbase. path:{}", outputPath);
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", hBaseProperties.getZkQuorum());
                    conf.set("hbase.zookeeper.property.clientPort", hBaseProperties.getZkPort());
                    LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(conf);
                    String[] args = new String[]{outputPath, loadBean.getTargetTableName()};
                    try {
                        loadIncrementalHFiles.run(args);
                        log.info("load success. path:{}", outputPath);
                    } catch (Exception e) {
                        log.error("load hfile error. path:{}", outputPath, e);
                    }
                }
            }
        });
        return "ok";
    }

    /**
     * 初始化YarnClient
     */
    private YarnClient initYarnClient() {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();

        YarnClient yarnClient = YarnClient.createYarnClient();

        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        return yarnClient;
    }

    /**
     * 提交完成后关闭YarnClient
     */
    private void closeYarnClient(YarnClient yarnClient) throws IOException {
        if (yarnClient != null) {
            yarnClient.stop();
            yarnClient.close();
        }
    }

    /**
     * 获取Yarn执行状态
     */
    private ApplicationReport getApplicationReport(YarnClient yarnClient, String jobId) throws IOException, YarnException {
        List<ApplicationReport> applications = yarnClient.getApplications();
        for (ApplicationReport applicationReport : applications) {
            String name = applicationReport.getName();
            if (name.equals(jobId)) {
                return applicationReport;
            }
        }
        return null;
    }

}
