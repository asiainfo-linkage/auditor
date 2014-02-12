package com.ailk.oci.auditor.server.util; /**
 *
 * The purpose of this class is to override the default behaviour of the spring JpaRepositoryFactory class.
 * It will produce a PartitioningSimpleJpaRepository object instead of SimpleJpaRepository.
 *
 */

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.support.*;
import org.springframework.data.querydsl.QueryDslPredicateExecutor;
import org.springframework.data.repository.core.RepositoryMetadata;

import javax.persistence.EntityManager;
import java.io.Serializable;

import static org.springframework.data.querydsl.QueryDslUtils.QUERY_DSL_PRESENT;


public class PartitioningJpaRepositoryFactory extends JpaRepositoryFactory {
    public PartitioningJpaRepositoryFactory(EntityManager entityManager) {
        super(entityManager);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <T, ID extends Serializable> JpaRepository<?, ?> getTargetRepository(
            RepositoryMetadata metadata, EntityManager entityManager) {
        Class<?> repositoryInterface = metadata.getRepositoryInterface();
        JpaEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());

        SimpleJpaRepository<?, ?> repo = isQueryDslExecutor(repositoryInterface) ? new QueryDslJpaRepository(
                entityInformation, entityManager) : new PartitioningSimpleJpaRepository(entityInformation, entityManager);
        repo.setLockMetadataProvider(LockModeRepositoryPostProcessor.INSTANCE.getLockMetadataProvider());
        return repo;
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        if (isQueryDslExecutor(metadata.getRepositoryInterface())) {
            return QueryDslJpaRepository.class;
        } else {
            return PartitioningSimpleJpaRepository.class;
        }
    }

    private boolean isQueryDslExecutor(Class<?> repositoryInterface) {
        return QUERY_DSL_PRESENT && QueryDslPredicateExecutor.class.isAssignableFrom(repositoryInterface);
    }
}