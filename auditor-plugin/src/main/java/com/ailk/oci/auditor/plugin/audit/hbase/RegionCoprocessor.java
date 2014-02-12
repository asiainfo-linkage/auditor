package com.ailk.oci.auditor.plugin.audit.hbase;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.hbase.util.Operation;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.ailk.oci.auditor.plugin.audit.hbase.util.PipelineUtil.in;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午6:56
 * To change this template use File | Settings | File Templates.
 */
public class RegionCoprocessor extends BaseRegionObserver {
    private final static Logger LOG = LoggerFactory.getLogger(RegionCoprocessor.class);
    private Pipeline pipeline;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        pipeline = new Pipeline(new File(e.getConfiguration().get("oci.auditor.com.ailk.oci.auditor.pipeline.hbase.region.config", "pipeline.properties")));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
        pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        in(pipeline, Operation.Region.OPEN, getTableName(e));
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        in(pipeline, Operation.Region.CLOSE, getTableName(e));
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        in(pipeline, Operation.Region.FLUSH, getTableName(e));
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
        in(pipeline, Operation.Region.FLUSH, getTableName(e));
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        in(pipeline, Operation.Region.SPLIT, getTableName(e));
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected, CompactionRequest request) {
        in(pipeline, Operation.Region.COMPACT_SELECTION, getTableName(c));
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected) {
        in(pipeline, Operation.Region.COMPACT_SELECTION, getTableName(c));
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
        in(pipeline, Operation.Region.COMPACT, getTableName(e));
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile, CompactionRequest request) throws IOException {
        in(pipeline, Operation.Region.COMPACT, getTableName(e));
    }

    @Override
    public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, Result result) throws IOException {
        in(pipeline, Operation.Region.GET_CLOSEST_ROW_BEFORE, getTableName(e), Bytes.toString(family));
    }

    @Override
    public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        in(pipeline, Operation.Region.GET, getTableName(e), get);
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists) throws IOException {
        in(pipeline, Operation.Region.EXISTS, getTableName(e), get);
        return super.postExists(e, get, exists);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        in(pipeline, Operation.Region.PUT, getTableName(e), put);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
        in(pipeline, Operation.Region.DELETE, getTableName(e), delete);
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
        in(pipeline, Operation.Region.BATCH_MUTATE, getTableName(c));
    }

    @Override
    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put, boolean result) throws IOException {
        in(pipeline, Operation.Region.CHECK_AND_PUT, getTableName(e), Bytes.toString(family), Bytes.toString(qualifier));
        return super.postCheckAndPut(e, row, family, qualifier, compareOp, comparator, put, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        in(pipeline, Operation.Region.CHECK_AND_DELETE, getTableName(e), Bytes.toString(family), Bytes.toString(qualifier));
        return super.postCheckAndDelete(e, row, family, qualifier, compareOp, comparator, delete, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append, Result result) throws IOException {
        in(pipeline, Operation.Region.APPEND, getTableName(e), append);
        return super.postAppend(e, append, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL, long result) throws IOException {
        in(pipeline, Operation.Region.INCREMENT_COLUMN_VALUE, getTableName(e), Bytes.toString(family), Bytes.toString(qualifier));
        return super.postIncrementColumnValue(e, row, family, qualifier, amount, writeToWAL, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment, Result result) throws IOException {
        in(pipeline, Operation.Region.INCREMENT, getTableName(e), increment);
        return super.postIncrement(e, increment, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        in(pipeline, Operation.Region.SCANNER_OPEN, getTableName(e), scan);
        return super.postScannerOpen(e, scan, s);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        in(pipeline, Operation.Region.SCANNER_NEXT, getTableName(e));
        return super.postScannerNext(e, s, results, limit, hasMore);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, byte[] currentRow, boolean hasMore) throws IOException {
        in(pipeline, Operation.Region.SCANNER_FILTER_ROW, getTableName(e));
        return super.postScannerFilterRow(e, s, currentRow, hasMore);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
        in(pipeline, Operation.Region.SCANNER_CLOSE, getTableName(e));
        super.postScannerClose(e, s);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        in(pipeline, Operation.Region.WAL_RESTORE, info.getTableNameAsString());
        super.postWALRestore(env, info, logKey, logEdit);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
        in(pipeline, Operation.Region.BULK_LOAD_HFILE, getTableName(ctx));
        return super.postBulkLoadHFile(ctx, familyPaths, hasLoaded);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void postLockRow(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] regionName, byte[] row) throws IOException {
        in(pipeline, Operation.Region.LOCK_ROW, getTableName(ctx));
        super.postLockRow(ctx, regionName, row);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void postUnlockRow(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] regionName, long lockId) throws IOException {
        in(pipeline, Operation.Region.UNLOCK_ROW, getTableName(ctx));
        super.postUnlockRow(ctx, regionName, lockId);    //To change body of overridden methods use File | Settings | File Templates.
    }

    private String getTableName(ObserverContext<RegionCoprocessorEnvironment> ctx) {
        String tableName = null;
        if (ctx != null) {
            RegionCoprocessorEnvironment environment = ctx.getEnvironment();
            if (environment != null) {
                HRegion region = ctx.getEnvironment().getRegion();
                if (region != null) {
                    HRegionInfo regionInfo = region.getRegionInfo();
                    if (regionInfo != null) {
                        tableName = regionInfo.getTableNameAsString();
                    }
                }
            }
        }
        return tableName;
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
                    HRegionServer.main(new String[]{"stop"});
                } catch (Exception e) {
                    LOG.warn("failed to stop hbase region server.", e);
                }
            }
        }
    }
}
