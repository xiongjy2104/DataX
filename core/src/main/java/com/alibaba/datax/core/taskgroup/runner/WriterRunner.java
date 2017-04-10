package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jingxing on 14-9-1.
 * <p/>
 * 单个slice的writer执行调用
 */
public class WriterRunner extends AbstractRunner implements Runnable {

    private static final Logger LOG = LoggerFactory
            .getLogger(WriterRunner.class);

    private RecordReceiver recordReceiver;

    public void setRecordReceiver(RecordReceiver receiver) {
        this.recordReceiver = receiver;
    }

    public WriterRunner(AbstractTaskPlugin abstractTaskPlugin) {
        super(abstractTaskPlugin);
    }

    @Override
    public void run() {
        Validate.isTrue(this.recordReceiver != null);

        Writer.Task taskWriter = (Writer.Task) this.getPlugin();
        //统计waitReadTime，并且在finally end
        PerfRecord channelWaitRead = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WAIT_READ_TIME);
        try {
            channelWaitRead.start();
            LOG.debug("task writer starts to do init ...");
            PerfRecord initPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_INIT);
            initPerfRecord.start();
            taskWriter.init();
            initPerfRecord.end();

            LOG.debug("task writer starts to do prepare ...");
            PerfRecord preparePerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_PREPARE);
            preparePerfRecord.start();
            taskWriter.prepare();
            preparePerfRecord.end();
            LOG.debug("task writer starts to write ...");

            PerfRecord dataPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_DATA);
            dataPerfRecord.start();
            taskWriter.startWrite(recordReceiver);

            dataPerfRecord.addCount(CommunicationTool.getTotalReadRecords(super.getRunnerCommunication()));
            dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
            dataPerfRecord.end();

            LOG.debug("task writer starts to save checkpoint ...");
            saveCheckpoint(((BufferedRecordExchanger)recordReceiver).getConfiguration());
       
            LOG.debug("task writer starts to do post ...");
            PerfRecord postPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_POST);
            postPerfRecord.start();
            taskWriter.post();
            postPerfRecord.end();

            super.markSuccess();
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:", e);
            super.markFail(e);
        } finally {
            LOG.debug("task writer starts to do destroy ...");
            PerfRecord desPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_DESTROY);
            desPerfRecord.start();
            super.destroy();
            desPerfRecord.end();
            channelWaitRead.end(super.getRunnerCommunication().getLongCounter(CommunicationTool.WAIT_READER_TIME));
        }
    }
    
    public static void saveCheckpoint(Configuration originalConfig) {
        Long jobId = originalConfig.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        if(jobId==0)
            jobId=-1L;
        LOG.info("jobId="+jobId);
        String checkpointPath=System.getProperty("sync.checkpoint.path");
        LOG.info("checkpointPath="+checkpointPath);
        String checkpointFile=(StringUtils.isBlank(checkpointPath)? CoreConstant.DATAX_BIN_HOME:checkpointPath)+"/job."+jobId+".checkpoint";
        LOG.info("checkpointFile="+checkpointFile);
        File file=new File(checkpointFile);
        FileOutputStream fos=null;
        Properties cpp=new Properties();
        try {
            Map<String, Object> map =originalConfig.getMap(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_CHECKPOINT);
            for (Object o : map.entrySet()) {
                Map.Entry entry = (Map.Entry) o;
                cpp.setProperty((String)entry.getKey(), String.valueOf(entry.getValue()));
            }
            if(!file.exists()){
                file.createNewFile();
            }
            fos=new FileOutputStream(file);
            cpp.store(fos,"checkpoints saved here.");
            fos.flush();
        } catch (Exception e) {
            LOG.info("Exception ="+e);
        }finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public boolean supportFailOver(){
    	Writer.Task taskWriter = (Writer.Task) this.getPlugin();
    	return taskWriter.supportFailOver();
    }

    public void shutdown(){
        recordReceiver.shutdown();
    }
}
