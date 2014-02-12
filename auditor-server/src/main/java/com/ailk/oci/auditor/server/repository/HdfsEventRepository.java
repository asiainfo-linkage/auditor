package com.ailk.oci.auditor.server.repository;

import com.ailk.oci.auditor.server.domain.HdfsEvent;
import com.ailk.oci.auditor.server.util.PartitioningJpaRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import javax.persistence.NamedQuery;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-5
 * Time: 上午11:17
 * To change this template use File | Settings | File Templates.
 */
public interface HdfsEventRepository extends PartitioningJpaRepository<HdfsEvent, Long>, JpaSpecificationExecutor<HdfsEvent> {
}
