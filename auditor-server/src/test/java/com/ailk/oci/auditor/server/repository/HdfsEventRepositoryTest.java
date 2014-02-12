package com.ailk.oci.auditor.server.repository;

import com.ailk.oci.auditor.server.Main;
import com.ailk.oci.auditor.server.domain.HdfsEvent;
import com.ailk.oci.auditor.server.util.PartitioningJpaRepository;
import junit.extensions.TestSetup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationContextLoader;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-5
 * Time: 下午2:57
 * To change this template use File | Settings | File Templates.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Main.class, loader = SpringApplicationContextLoader.class)
public class HdfsEventRepositoryTest {
    @Autowired
    HdfsEventRepository repository;

    @Before
    public void setup(){
    }

    @Test
    public void saveInPartitionAndSelectInBaseTable() {


        HdfsEvent hdfsEvent1 = createEvent(System.currentTimeMillis(), "1");
        HdfsEvent hdfsEvent1_1 = createEvent(System.currentTimeMillis(), "1");
        HdfsEvent hdfsEvent2 = createEvent(System.currentTimeMillis() - 1 * 24 * 60 * 60 * 1000, "2");
        HdfsEvent hdfsEvent3 = createEvent(System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000, "1");
        HdfsEvent hdfsEvent4 = createEvent(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000, "1");
        HdfsEvent hdfsEvent5 = createEvent(System.currentTimeMillis() - 4 * 24 * 60 * 60 * 1000, "1");
        HdfsEvent hdfsEvent6 = createEvent(System.currentTimeMillis() - 5 * 24 * 60 * 60 * 1000, "1");

        repository.persist(hdfsEvent1, hdfsEvent1_1, hdfsEvent2, hdfsEvent3, hdfsEvent4, hdfsEvent5, hdfsEvent6);


        String[] orderBy = new String[]{"localTime"};
        String[] direction = new String[]{"desc"};
        HdfsEvent hdfsEvent = new HdfsEvent();
        hdfsEvent.setDst("1");

        int page = 0;
        int size = 10;

        Date end = new Date();
        Date start = new Date(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000);

        List<Sort.Order> orders = new ArrayList<Sort.Order>();
        for (int i = 0; i < orderBy.length; i++) {
            orders.add(new Sort.Order(Sort.Direction.fromString(direction[i]), orderBy[i]));
        }
        List<HdfsEvent> eventList = repository.findAll(hdfsEvent, new PageRequest(page, size), new Sort(orders), new PartitioningJpaRepository.Param[]{new PartitioningJpaRepository.Param("localTime", ">", start), new PartitioningJpaRepository.Param("localTime", "<", end)});

        Assert.assertEquals(3, eventList.size());
    }

    private HdfsEvent createEvent(long time, String dst) {
        HdfsEvent hdfsEvent = new HdfsEvent();
        hdfsEvent.setLocalTime(new Date(time));
        hdfsEvent.setDst(dst);
        return hdfsEvent;
    }
}
