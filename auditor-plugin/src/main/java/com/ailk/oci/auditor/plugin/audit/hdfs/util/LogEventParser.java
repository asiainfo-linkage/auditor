package com.ailk.oci.auditor.plugin.audit.hdfs.util;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent;

import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午1:57
 * To change this template use File | Settings | File Templates.
 */
public class LogEventParser implements Pipeline.InProcessor {
//    private Pipeline pipeline;
	private String serviceName;
    private final static Logger LOG = LoggerFactory.getLogger(LogEventParser.class);

    /**
     * //logEvent格式参考：org.apache.hadoop.hdfs.server.namenode.FSNamesystem.DefaultAuditLogger.logAuditEvent()
     * final StringBuilder sb = auditBuffer.get();
     * sb.setLength(0);
     * sb.append("allowed=").append(succeeded).append("\t");
     * sb.append("ugi=").append(userName).append("\t");
     * sb.append("ip=").append(addr).append("\t");
     * sb.append("cmd=").append(cmd).append("\t");
     * sb.append("src=").append(src).append("\t");
     * sb.append("dst=").append(dst).append("\t");
     * if (null == status) {
     * sb.append("perm=null");
     * } else {
     * sb.append("perm=");
     * sb.append(status.getOwner()).append(":");
     * sb.append(status.getGroup()).append(":");
     * sb.append(status.getPermission());
     * }
     * auditLog.info(sb);
     *
     * @param elements
     * @return
     */
    @Override
    public Object[] onElements(Object... elements) {
        Object[] processedElements = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (!(element instanceof LoggingEvent)) {
                processedElements[i] = element;// 不处理非LogEvent,应该要吗？？ZhangLi
            } else {
                LoggingEvent logEvent = (LoggingEvent) element;
                String message = logEvent.getMessage().toString();
                long time = logEvent.getTimeStamp();


                Properties properties = new Properties();
                String[] entries = message.split("\t");
                for (String entryString : entries) {
                    String[] entry = entryString.split("=", 2);
                    if (entry.length == 2) {
                        properties.setProperty(entry[0], "null".equals(entry[1]) ? "null" : entry[1]);
                    }
                }
                String perm = properties.getProperty("perm");
                if (perm != null) {
                    String[] permValues = perm.split(":", 3);
                    if (permValues.length == 3) {
                        properties.setProperty("owner", permValues[0]);
                        properties.setProperty("group", permValues[1]);
                        properties.setProperty("permission", permValues[2]);
                    }
                }
                UserInfo userInfo = new UserInfo(properties.getProperty("ugi"));

                String[] address = parseIpAddress(properties.getProperty("ip"));
                Event event = Event.newBuilder()
//                        .setService(this.pipeline.getServiceName())
                		.setService(this.serviceName)
                        .setLocalTime(time)
                        .setAllowed(Boolean.parseBoolean(properties.getProperty("allowed", "false")))
                        .setUser(userInfo.getUsername())
                        .setImpersonator(userInfo.getImpersonator())
                        .setHostName(address[0])
                        .setHostAddress(address[1])
                        .setOperation(properties.getProperty("cmd"))
                        .setEvent(HdfsEvent.newBuilder()
                                .setSrc(properties.getProperty("src"))
                                .setDst(properties.getProperty("dst"))
                                .setOwner(properties.getProperty("owner"))
                                .setGroup(properties.getProperty("group"))
                                .setPermissions(properties.getProperty("permission"))
                                .build())
                        .build();
                processedElements[i] = event;
                
                LOG.debug(String.format("[ZhangLi]HDFS-Event parsed. opration=[%s] user=[%s]", event.getOperation(),event.getUser()));
            }
        }
        return processedElements;
    }

    protected String[] parseIpAddress(String raw) {
        if (raw != null) {
//        	if(raw.startsWith("/")){//过滤掉开头的/
//        		raw = raw.substring(1,raw.length()-1);
//        		
//        	}
            int sep = raw.indexOf('/');
            if ((sep >= 0) && (sep < raw.length() - 1)) {
                return new String[]{raw.substring(0, sep), raw.substring(sep + 1)};
            }else{
            	return new String[]{raw,""};
            }
        }
        return new String[2];
    }

    @Override
    public void updateConfig(Properties properties) {
        LOG.debug("LogEventParser.updateConfig");
        this.serviceName = properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.service.name", "unknown");
    }

    @Override
    public void close() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
