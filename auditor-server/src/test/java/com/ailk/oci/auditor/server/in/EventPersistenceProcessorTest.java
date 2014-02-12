package com.ailk.oci.auditor.server.in;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.springframework.beans.factory.annotation.Autowired;

import com.ailk.oci.auditor.server.domain.HdfsEvent;

public class EventPersistenceProcessorTest {
	@Autowired
	EventPersistenceProcessor processor;

    @Before
    public void setup(){
    }

    @Ignore
    public void saveInPartitionAndSelectInBaseTable() {
    	 HdfsEvent hdfsEvent1 = createEvent(System.currentTimeMillis(), "1");
    	 HdfsEvent hdfsEvent2 = createEvent(System.currentTimeMillis(), "2");
    	 processor.onElements(hdfsEvent1,hdfsEvent2);
    	 
    }
    
    
    private HdfsEvent createEvent(long time, String dst) {
        HdfsEvent hdfsEvent = new HdfsEvent();
        hdfsEvent.setLocalTime(new Date(time));
        hdfsEvent.setDst(dst);
        return hdfsEvent;
    }
}
