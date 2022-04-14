package com.allsenseww.hbase.service.bulkload;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;

import java.util.List;

import static org.apache.flink.configuration.CoreOptions.FLINK_TM_JVM_OPTIONS;

public class FlinkEnvironmentBuild {

    private final Configuration configuration;

    public FlinkEnvironmentBuild() {
        this.configuration = buildDefaultConfiguration();
    }

    public FlinkEnvironmentBuild setTaskMemSize(String memSize) {
        this.configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse(memSize));
        return this;
    }

    public FlinkEnvironmentBuild setJobMemSize(String memSize) {
        this.configuration.set(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, memSize);
        return this;
    }

    public FlinkEnvironmentBuild setFlinkDistJar(String flinkDistJar) {
        this.configuration.setString(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar);
        return this;
    }

    public FlinkEnvironmentBuild setUserJars(List<String> userJars) {
        try {
            configuration.set(PipelineOptions.JARS, userJars);
        } catch (Exception e) {
            throw new RuntimeException("not found the jar.");
        }
        return this;
    }

    public FlinkEnvironmentBuild setTMJVMOptions(String options) {
        this.configuration.set(FLINK_TM_JVM_OPTIONS, options);
        return this;
    }

    public FlinkEnvironmentBuild setTaskId(String taskId) {
        configuration.setString(YarnConfigOptions.APPLICATION_NAME, taskId);
        return this;
    }

    private Configuration buildDefaultConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(DeploymentOptions.TARGET, YarnJobClusterExecutor.NAME);

        configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("1024m"));
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("128m"));

        configuration.set(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, "1024m");

        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        configuration.setBoolean(DeploymentOptions.ATTACHED, false);

        configuration.setInteger(YarnConfigOptions.APP_MASTER_VCORES, 1);
        configuration.set(FLINK_TM_JVM_OPTIONS, "-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native");
        return configuration;
    }

    public ExecutionEnvironment build() {
        return new ExecutionEnvironment(DefaultExecutorServiceLoader.INSTANCE, configuration, this.getClass().getClassLoader());
    }

}
