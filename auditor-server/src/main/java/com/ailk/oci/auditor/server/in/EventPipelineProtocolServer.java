package com.ailk.oci.auditor.server.in;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.EventPipelineProtocol;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.WriteRequest;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.WriteResponse;
import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;
import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-4
 * Time: 下午6:58
 * To change this template use File | Settings | File Templates.
 */
public class EventPipelineProtocolServer implements EventPipelineProtocol {
    private final static Logger LOG = LoggerFactory.getLogger(EventPipelineProtocolServer.class);
    @Value("${oci.auditor.com.ailk.oci.auditor.pipeline.server.config}")
    private String config;
    private Pipeline pipeline;

    @Autowired
    @Resource( name = "eventPersistenceProcessor")
    private Pipeline.OutProcessor eventPersistenceProcessor;

    @Override
    public WriteResponse write(WriteRequest request) throws AvroRemoteException {
        if (pipeline == null) {
            synchronized (this) {
                if (pipeline == null) {
                    pipeline = new Pipeline(new File(config));
                    pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
                    pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
                    pipeline.setOutProcessors(eventPersistenceProcessor);
                    pipeline.updateConfig4Processors();
                }
            }
        }
        pipeline.in(request.getEvents().toArray());
        WriteResponse response = WriteResponse.newBuilder().setSuccess(true).build();
        return response;
    }

    private static class QueueFullAction implements Pipeline.Action {
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
                LOG.warn("auditor server audit logger [" + code.name() + "], terminating ...");
                System.exit(2);
            }
        }
    }
}
