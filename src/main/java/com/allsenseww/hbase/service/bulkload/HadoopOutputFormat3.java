package com.allsenseww.hbase.service.bulkload;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormatBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase.getCredentialsFromUGI;

public class HadoopOutputFormat3<ImmutableBytesWritable, Cell> extends HadoopOutputFormatBase<org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.Cell, Tuple2<org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.Cell>> {

//    private byte[] salt = null;

    private int perBucket = 8;
    private static final long serialVersionUID = 1L;
    private int numTasks;
    private int originTaskNum;

    public HadoopOutputFormat3(OutputFormat<org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.Cell> mapreduceOutputFormat, Job job) {
        super(mapreduceOutputFormat, job);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // enforce sequential open() calls
        synchronized (OPEN_MUTEX) {
            if (Integer.toString(taskNumber + 1).length() > 6) {
                throw new IOException("Task id too large.");
            }

            this.taskNumber = taskNumber + 1;
            this.numTasks = numTasks;
            this.originTaskNum = taskNumber;

            // for hadoop 2.2
            this.configuration.set("mapreduce.output.basename", "tmp");

            TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
                    + String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s", " ").replace(" ", "0")
                    + Integer.toString(taskNumber + 1)
                    + "_0");

            this.configuration.set("mapred.task.id", taskAttemptID.toString());
            this.configuration.setInt("mapred.task.partition", taskNumber + 1);
            // for hadoop 2.2
            this.configuration.set("mapreduce.task.attempt.id", taskAttemptID.toString());
            this.configuration.setInt("mapreduce.task.partition", taskNumber + 1);

            try {
                this.context = new TaskAttemptContextImpl(this.configuration, taskAttemptID);
                this.outputCommitter = this.mapreduceOutputFormat.getOutputCommitter(this.context);
                this.outputCommitter.setupJob(new JobContextImpl(this.configuration, new JobID()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            this.context.getCredentials().addAll(this.credentials);
            Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
            if (currentUserCreds != null) {
                this.context.getCredentials().addAll(currentUserCreds);
            }

            // compatible for hadoop 2.2.0, the temporary output directory is different from hadoop 1.2.1
            if (outputCommitter instanceof FileOutputCommitter) {
                this.configuration.set("mapreduce.task.output.dir", ((FileOutputCommitter) this.outputCommitter).getWorkPath().toString());
            }

            try {
                this.recordWriter = this.mapreduceOutputFormat.getRecordWriter(this.context);
            } catch (InterruptedException e) {
                throw new IOException("Could not create RecordWriter.", e);
            }
        }
    }

    @Override
    public void writeRecord(Tuple2<org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.Cell> record) throws IOException {
        byte[] rowkey = record.f0.get();
        byte[] prefix = Arrays.copyOf(rowkey, 2);
        byte[] bucketByte = new byte[]{0, 0, prefix[0], prefix[1]};
        int bucket = Bytes.toInt(bucketByte);

        if (bucket == perBucket) {
            close();
            open(originTaskNum, numTasks);
            perBucket += 8;
        }

        try {
            this.recordWriter.write(record.f0, record.f1);
        } catch (InterruptedException e) {
            throw new IOException("Could not write Record.", e);
        }
    }

    @Override
    public void close() throws IOException {

        // enforce sequential close() calls
        synchronized (CLOSE_MUTEX) {
            try {
                this.recordWriter.close(this.context);
            } catch (InterruptedException e) {
                throw new IOException("Could not close RecordReader.", e);
            }

            if (this.outputCommitter.needsTaskCommit(this.context)) {
                this.outputCommitter.commitTask(this.context);
            }

            Path outputPath = new Path(this.configuration.get("mapred.output.dir"));

            // rename tmp-file to final name
            FileSystem fs = FileSystem.get(outputPath.toUri(), this.configuration);

            String taskNumberStr = Integer.toString(this.taskNumber);
            String tmpFileTemplate = "tmp-r-00000";
            String tmpFile = tmpFileTemplate.substring(0, 11 - taskNumberStr.length()) + taskNumberStr;

            if (fs.exists(new Path(outputPath.toString() + "/" + tmpFile))) {
                fs.rename(new Path(outputPath.toString() + "/" + tmpFile), new Path(outputPath.toString() + "/" + taskNumberStr));
            }
        }
    }

    @Override
    public void finalizeGlobal(int parallelism) throws IOException {

        JobContext jobContext;
        TaskAttemptContext taskContext;
        try {
            TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
                    + String.format("%" + (6 - Integer.toString(1).length()) + "s", " ").replace(" ", "0")
                    + Integer.toString(1)
                    + "_0");

            jobContext = new JobContextImpl(this.configuration, new JobID());
            taskContext = new TaskAttemptContextImpl(this.configuration, taskAttemptID);
            this.outputCommitter = this.mapreduceOutputFormat.getOutputCommitter(taskContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        jobContext.getCredentials().addAll(this.credentials);
        Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
        if (currentUserCreds != null) {
            jobContext.getCredentials().addAll(currentUserCreds);
        }

        // finalize HDFS output format
        if (this.outputCommitter != null) {
            this.outputCommitter.commitJob(jobContext);
        }
    }
}