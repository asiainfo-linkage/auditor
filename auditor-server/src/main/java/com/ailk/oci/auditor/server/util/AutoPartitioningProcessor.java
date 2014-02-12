package com.ailk.oci.auditor.server.util;

import com.ailk.oci.auditor.server.repository.HdfsEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-6
 * Time: 下午3:36
 * To change this template use File | Settings | File Templates.
 */
public class AutoPartitioningProcessor {
    @Autowired
    HdfsEventRepository repository;

    @PostConstruct
    @Scheduled(fixedDelay = 100000L)
    @Transactional
    public void send() {
        processsPartitions();
    }

    private void processsPartitions() {
        repository.processPartitions();
    }
}
