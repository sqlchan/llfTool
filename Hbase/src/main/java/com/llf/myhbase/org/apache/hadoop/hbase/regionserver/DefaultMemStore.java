package com.llf.myhbase.org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.ReflectionUtils;

public class DefaultMemStore {

    volatile MemStoreLAB allocator;
    volatile MemStoreLAB snapshotAllocator;

    public DefaultMemStore() {
        Configuration conf = new Configuration();
        String className = "hbase.regionserver.mslab.class";
        this.allocator = ReflectionUtils.instantiateWithCustomCtor(className,
                new Class[] { Configuration.class }, new Object[] { conf });
    }

    public void snapshot() {
        this.snapshotAllocator = this.allocator;
        Configuration conf = new Configuration();
        String className = "hbase.regionserver.mslab.class";
        this.allocator = ReflectionUtils.instantiateWithCustomCtor(className,
                new Class[] { Configuration.class }, new Object[] { conf });
    }
}
