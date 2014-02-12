package com.ailk.oci.auditor.plugin.audit.hbase;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.hbase.util.Operation;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.ailk.oci.auditor.plugin.audit.hbase.util.PipelineUtil.in;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午4:39
 * To change this template use File | Settings | File Templates.
 */
public class MasterCoprocessor extends BaseMasterObserver {
    private final static Logger LOG = LoggerFactory.getLogger(MasterCoprocessor.class);
    private Pipeline pipeline;

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        pipeline = new Pipeline(new File(ctx.getConfiguration().get("oci.auditor.com.ailk.oci.auditor.pipeline.hbase.master.config", "pipeline.properties")));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
        pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        in(pipeline, Operation.Master.CREATE_TABLE, desc.getNameAsString());
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
        in(pipeline, Operation.Master.DELETE_TABLE, Bytes.toString(tableName));
    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd) throws IOException {
        in(pipeline, Operation.Master.MODIFY_TABLE, Bytes.toString(tableName));
    }

    @Override
    public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HColumnDescriptor column) throws IOException {
        in(pipeline, Operation.Master.ADD_COLUMN, Bytes.toString(tableName), column.getNameAsString());
    }

    @Override
    public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HColumnDescriptor descriptor) throws IOException {
        in(pipeline, Operation.Master.MODIFY_COLUMN, Bytes.toString(tableName), descriptor.getNameAsString());
    }

    @Override
    public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, byte[] c) throws IOException {
        in(pipeline, Operation.Master.DELETE_COLUMN, Bytes.toString(tableName), Bytes.toString(c));
    }

    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
        in(pipeline, Operation.Master.ENABLE_TABLE, Bytes.toString(tableName));
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
        in(pipeline, Operation.Master.DISABLE_TABLE, Bytes.toString(tableName));
    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
        in(pipeline, Operation.Master.ASSIGN, regionInfo.getTableNameAsString());
    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo, boolean force) throws IOException {
        in(pipeline, Operation.Master.UN_ASSIGN, regionInfo.getTableNameAsString());
    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        in(pipeline, Operation.Master.BALANCE);
    }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean oldValue, boolean newValue) throws IOException {
        in(pipeline, Operation.Master.BALANCE_SWITCH);
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        in(pipeline, Operation.Master.START_MASTER);
    }


    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
        in(pipeline, Operation.Master.MOVE, region.getTableNameAsString());
    }

    @Override
    public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
        in(pipeline, Operation.Master.SNAPSHOT, hTableDescriptor.getNameAsString());
    }

    @Override
    public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
        in(pipeline, Operation.Master.CLONE_SNAPSHOT, hTableDescriptor.getNameAsString());
    }

    @Override
    public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
        in(pipeline, Operation.Master.RESTORE_SNAPSHOT, hTableDescriptor.getNameAsString());
    }

    @Override
    public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, HBaseProtos.SnapshotDescription snapshot) throws IOException {
        in(pipeline, Operation.Master.DELETE_SNAPSHOT, snapshot.getTable());
    }


    public static class QueueFullAction implements Pipeline.Action {
        @Override
        public void action(Pipeline pipeline, Pipeline.ActionCode code) {
            if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_DROP)) {
                LOG.warn("Hbase audit logger [" + code.name() + "], dropped");
            } else if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_SHUTDOWN)) {
                try {
                    pipeline.close();
                } catch (Exception e) {
                    LOG.warn("failed to close com.ailk.oci.auditor.pipeline", e);
                }
                try {
                    HMaster.main(new String[]{"stop"});
                } catch (Exception e) {
                    LOG.warn("failed to stop hbase master.", e);
                }
            }

        }
    }
}
