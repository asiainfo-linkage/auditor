package com.ailk.oci.auditor.plugin.audit.hdfs;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.hdfs.util.LogEventParser;
import com.ailk.oci.auditor.plugin.audit.util.LogToPipelineAppender;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午4:27
 * To change this template use File | Settings | File Templates.
 */
public class AuditLoggerAppender extends LogToPipelineAppender {
    public void activateOptions() {
        pipeline = new Pipeline(new File(config));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new AuditLoggerImpl.QueueFullAction());
        pipeline.setInProcessors(new LogEventParser(), new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }
}
