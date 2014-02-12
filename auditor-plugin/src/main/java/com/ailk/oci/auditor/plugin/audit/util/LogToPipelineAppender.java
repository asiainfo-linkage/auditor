package com.ailk.oci.auditor.plugin.audit.util;

import com.ailk.oci.auditor.pipeline.Pipeline;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午4:13
 * To change this template use File | Settings | File Templates.
 */
public class LogToPipelineAppender extends AppenderSkeleton {
    private final static Logger LOG = LoggerFactory.getLogger(LogToPipelineAppender.class);
    protected Pipeline pipeline;
    protected String config;

    @Override
    protected void append(LoggingEvent event) {
        if (this.pipeline != null && event.getMessage() != null) {
            this.pipeline.in(event);
        }
    }

    /**
     * Log4j支持自动注入属性
     *
     * @param config
     */
    public void setConfig(String config) {
        this.config = config;
    }

    @Override
    public void close() {
        if (this.pipeline != null)
            try {
                this.pipeline.close();
            } catch (Exception e) {
                LOG.warn("failed to close pipeline.", e);
            }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
