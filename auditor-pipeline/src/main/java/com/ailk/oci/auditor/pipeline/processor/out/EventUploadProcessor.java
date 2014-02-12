package com.ailk.oci.auditor.pipeline.processor.out;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.EventPipelineProtocol;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.WriteRequest;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.WriteResponse;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-30
 * Time: 下午1:54
 * To change this template use File | Settings | File Templates.
 */
public class EventUploadProcessor implements Pipeline.OutProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(EventUploadProcessor.class);
    private String serverUrl = "";
    private int timeout;
    private HttpTransceiver client;
    private EventPipelineProtocol proxy;

    @Override
    public Object[] onElements(Object... elements) {
        if (elements.length > 0) {
            List events = new ArrayList(elements.length);
            for (Object element : elements) {
                events.add(element);
            }
            WriteRequest writeRequest = WriteRequest.newBuilder().setEvents(events).build();
            
            try {
                WriteResponse response = this.proxy.write(writeRequest);
                if (response.getSuccess()) {
                    return ArrayUtils.EMPTY_OBJECT_ARRAY;
                } else {
                    return elements;
                }
            } catch (AvroRemoteException e) {
                LOG.warn("failed to upload events", e);
                return elements;
            }
        }
        return ArrayUtils.EMPTY_OBJECT_ARRAY;
    }

    @Override
    public void updateConfig(Properties properties) {
        String serverUrl = properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.processor.upload.server.url", "");
    	if (!serverUrl.equals(this.serverUrl)) {
            LOG.info(String.format("serverUrl changed from [%s] to [%s] . (oci.auditor.com.ailk.oci.auditor.pipeline.config.processor.upload.server.url)", this.serverUrl, serverUrl));
            if (serverUrl.equals("")) {
                this.client = null;
                this.serverUrl = serverUrl;
            } else {
                try {
                    HttpTransceiver client = new HttpTransceiver(new URL(serverUrl));
                    EventPipelineProtocol proxy = SpecificRequestor.getClient(EventPipelineProtocol.class, client);//this.proxy = SpecificRequestor.getClient(EventPipelineProtocol.class, client);
                    this.serverUrl = serverUrl;
                    this.proxy = proxy;
                    if (this.client != null) {
                        try {
                            this.client.close();
                        } catch (IOException e) {
                            LOG.warn("failed to close old client", e);
                        }
                    }
                    this.client = client;
                } catch (MalformedURLException e) {
                    LOG.warn("serverUrl format error,continue use old serverUrl", e);
                } catch (IOException e) {
                    LOG.warn("serverUrl error,continue use old serverUrl", e);
                }catch(Exception e){
                	LOG.warn("Excetpion in updateConfig.", e);
                }
            }
        }

        if (this.client != null) {
            try {
                int timeout = Integer.parseInt(properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.processor.upload.server.timeout", "3000"));
                LOG.info(String.format("timeout changed from [%s] to [%s] . (oci.auditor.com.ailk.oci.auditor.pipeline.config.processor.upload.server.timeout)", this.timeout, timeout));
                this.client.setTimeout(timeout);
                this.timeout = timeout;
            } catch (NumberFormatException e) {
                LOG.warn("timeout format error,continue use old timeout", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (IOException e) {
                LOG.warn("failed to close client", e);
            }
        }
    }
}
