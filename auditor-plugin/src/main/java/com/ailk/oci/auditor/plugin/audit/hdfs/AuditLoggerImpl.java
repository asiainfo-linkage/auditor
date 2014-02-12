package com.ailk.oci.auditor.plugin.audit.hdfs;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-29
 * Time: 下午4:58
 * To change this template use File | Settings | File Templates.
 */
public class AuditLoggerImpl implements AuditLogger {
    private final static Logger LOG = LoggerFactory.getLogger(AuditLoggerImpl.class);
    private Pipeline pipeline;

    @Override
    public void initialize(Configuration conf) {
        pipeline = new Pipeline(new File(conf.get("oci.auditor.com.ailk.oci.auditor.pipeline.hdfs.config", "pipeline.properties")));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
        pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
                              InetAddress addr, String cmd, String src, String dst,
                              FileStatus stat) {
        UserInfo userInfo = new UserInfo(userName);
        Event event = Event.newBuilder()
                .setService(this.pipeline.getServiceName())
                .setLocalTime(System.currentTimeMillis())
                .setAllowed(succeeded)
                .setUser(userInfo.getUsername())
                .setImpersonator(userInfo.getImpersonator())
                .setHostName(addr != null ? addr.getHostName() : null)
                .setHostAddress(addr != null ? addr.getHostAddress() : null)
                .setOperation(cmd)
                .setEvent(HdfsEvent.newBuilder()
                        .setSrc(src)
                        .setDst(dst)
                        .setOwner(stat == null ? null : stat.getOwner())
                        .setGroup(stat == null ? null : stat.getGroup())
                        .setPermissions(stat == null ? null : stat.getPermission().toString())
                        .build())
                .build();
        this.pipeline.in(event);
    }

    public static class QueueFullAction implements Pipeline.Action {
        @Override
        public void action(Pipeline pipeline, Pipeline.ActionCode code) {
            if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_DROP)) {
                LOG.warn("Hdfs audit logger [" + code.name() + "], dropped");
            } else if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_SHUTDOWN)) {
                try {
                    pipeline.close();
                } catch (Exception e) {
                    LOG.warn("failed to close com.ailk.oci.auditor.pipeline", e);
                }
                ExitUtil.terminate(2, "Hdfs audit logger [" + code.name() + "], terminating ...");
            }
        }
    }
}
