package com.ailk.oci.auditor.plugin.audit.hbase.util;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile("Access (allowed|denied).*");

    /**
     * //logEvent格式参考：org.apache.hadoop.hbase.security.access.AccessController#logResult
     * //org.apache.hadoop.hbase.security.access.AuthResult#toContextString()
     *
     * @param elements
     * @return
     */
    @Override
    public Object[] onElements(Object... elements) {
        Object[] processedElements = new Object[0];
        int filteredCount = 0; //过滤掉的元素，比如不关心的日志
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (!(element instanceof LoggingEvent)) {
                processedElements[i] = element;// 不处理非LogEvent
            } else {
                LoggingEvent logEvent = (LoggingEvent) element;
                String message = logEvent.getMessage().toString();
                long time = logEvent.getTimeStamp();

                //在Hbase中使用SecurityLogger打了很多种日志，我们只关心Access日志
                Matcher matcher = ACCESS_LOG_PATTERN.matcher(message);
                if (matcher.matches()) {
                    Properties properties = new Properties();
                    properties.setProperty("allowed", Boolean.toString(matcher.group(1).equals("allowed")));
                    String[] entries = message.split(";", 5);
                    for (int j = 1; j < entries.length; j++) {
                        String[] entry = entries[i].split(":", 2);
                        if (entry.length == 2) {
                            properties.setProperty(entry[0], "null".equals(entry[1]) ? null : entry[1]);
                        }
                    }
                    //org.apache.hadoop.hbase.security.access.AuthResult.toContextString()
                    String context = properties.getProperty("context", "()");
                    String entriesString = context.substring(context.indexOf('(') + 1, context.lastIndexOf(')'));
                    entries = entriesString.split(",", 4);
                    for (int j = 1; j < entries.length; j++) {
                        String[] entry = entries[i].split("=", 2);
                        if (entry.length == 2) {
                            properties.setProperty(entry[0], "null".equals(entry[1]) ? "null" : entry[1]);
                        }
                    }

                    UserInfo userInfo = new UserInfo(properties.getProperty("user"));
                    String[] address = parseIpAddress(properties.getProperty("remote address"));


                    processedElements[i] = Event.newBuilder()
                            .setAllowed(Boolean.parseBoolean(properties.getProperty("allowed")))
//                            .setService(pipeline.getServiceName())
                            .setService(serviceName)
                            .setUser(userInfo.getUsername())
                            .setImpersonator(userInfo.getImpersonator())
                            .setHostName(address[0])
                            .setHostAddress(address[1])
                            .setLocalTime(time)
                            .setOperation(properties.getProperty("action"))//READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A');
                            .setEvent(HBaseEvent.newBuilder()
                                    .setTable(properties.getProperty("scope"))
                                    .setFamily(properties.getProperty("family"))
                                    .setQualifier(properties.getProperty("qualifier"))
                                    .build())
                            .build();
                } else {
                    filteredCount++;
                    processedElements[i] = null;
                }
            }
        }
        return removeNull(processedElements, filteredCount);
    }

    private Object[] removeNull(Object[] processedElements, int filteredCount) {
        Object[] result = new Object[processedElements.length - filteredCount];
        int j = 0;
        for (int i = 0; i < result.length; i++) {
            for (; j < processedElements.length; j++) {
                if (processedElements[j] != null) {
                    result[i] = processedElements[j++];
                    break;
                }
            }
        }
        return result;
    }

    protected String[] parseIpAddress(String raw) {
        if (raw != null) {
            int sep = raw.indexOf('/');
            if ((sep >= 0) && (sep < raw.length() - 1)) {
                return new String[]{raw.substring(0, sep), raw.substring(sep + 1)};
            }
        }
        return new String[2];
    }

    @Override
    public void updateConfig(Properties properties) {
        //To change body of implemented methods use File | Settings | File Templates.
    	this.serviceName = properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.service.name", "unknown");
    }

    @Override
    public void close() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
