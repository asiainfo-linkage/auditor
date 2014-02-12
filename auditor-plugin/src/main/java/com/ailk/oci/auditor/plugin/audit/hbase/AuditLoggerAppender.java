package com.ailk.oci.auditor.plugin.audit.hbase;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.hbase.util.LogEventParser;
import com.ailk.oci.auditor.plugin.audit.util.LogToPipelineAppender;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午4:27
 * To change this template use File | Settings | File Templates.
 */
public class AuditLoggerAppender extends LogToPipelineAppender {
    private final static Logger LOG = LoggerFactory.getLogger(RegionCoprocessor.class);

    public void activateOptions() {
        pipeline = new Pipeline(new File(config));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
        pipeline.setInProcessors(new LogEventParser(), new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }

    public static class QueueFullAction implements Pipeline.Action {
        @Override
        public void action(Pipeline pipeline, Pipeline.ActionCode code) {
            if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_DROP)) {
                LOG.warn("Hbase audit logger [" + code.name() + "], dropped");
            } else if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_SHUTDOWN)) {
                try {
                    pipeline.close();
                } catch (Exception e) {
                    LOG.warn("failed to close com.ailk.oci.auditor.pipeline", e);
                }
                ExitUtil.terminate(2, "Hbase audit logger [" + code.name() + "], terminating ...");
            }

        }
    }
}
