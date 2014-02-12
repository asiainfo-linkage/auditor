package com.ailk.oci.auditor.server.repository;

import com.ailk.oci.auditor.server.domain.HBaseEvent;
import com.ailk.oci.auditor.server.util.PartitioningJpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-5
 * Time: 上午11:17
 * To change this template use File | Settings | File Templates.
 */
public interface HBaseEventRepository extends PartitioningJpaRepository<HBaseEvent, Long>, JpaSpecificationExecutor<HBaseEvent> {
}
