/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sql.hbase.nosleep.mapReduce.rowcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * A job with a just a map phase to count rows. Map outputs table rows IF the
 * input row has columns that have content.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RowCounter {

  /** Name of this 'program'. */
  static final String NAME = "rowcounter";

  /**
   * Mapper that runs the count.
   */
  static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {

    /** Counter enumeration to count the actual rows. 计数器枚举可对实际行进行计数。*/
    public static enum Counters {ROWS}

    /**
     * Maps the data.
     *
     * @param row  The current table row key.
     * @param values  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values,
                    Context context)
    throws IOException {
      // Count every row containing data, whether it's in qualifiers or values
      // 计算包含数据的每一行，无论是在限定符还是在值中
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    String tableName = args[0];
    String startKey = null;
    String endKey = null;
    long startTime = 0;
    long endTime = 0;

    StringBuilder sb = new StringBuilder();

    final String rangeSwitch = "--range=";
    final String startTimeArgKey = "--starttime=";
    final String endTimeArgKey = "--endtime=";

    // First argument is table name, starting from second
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith(rangeSwitch)) {
        String[] startEnd = args[i].substring(rangeSwitch.length()).split(",", 2);
        if (startEnd.length != 2 || startEnd[1].contains(",")) {
          printUsage("Please specify range in such format as \"--range=a,b\" " +
              "or, with only one boundary, \"--range=,b\" or \"--range=a,\"");
          return null;
        }
        startKey = startEnd[0];
        endKey = startEnd[1];
      }
      if (startTime < endTime) {
        printUsage("--endtime=" + endTime + " needs to be greater than --starttime=" + startTime);
        return null;
      }
      if (args[i].startsWith(startTimeArgKey)) {
        startTime = Long.parseLong(args[i].substring(startTimeArgKey.length()));
        continue;
      }
      if (args[i].startsWith(endTimeArgKey)) {
        endTime = Long.parseLong(args[i].substring(endTimeArgKey.length()));
        continue;
      }
      else {
        // if no switch, assume column names
        sb.append(args[i]);
        sb.append(" ");
      }
    }

    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(org.apache.hadoop.hbase.mapreduce.RowCounter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    Set<byte []> qualifiers = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    if (startKey != null && !startKey.equals("")) {
      scan.setStartRow(Bytes.toBytes(startKey));
    }
    if (endKey != null && !endKey.equals("")) {
      scan.setStopRow(Bytes.toBytes(endKey));
    }
    if (sb.length() > 0) {
      for (String columnName : sb.toString().trim().split(" ")) {
        String family = StringUtils.substringBefore(columnName, ":");
        String qualifier = StringUtils.substringAfter(columnName, ":");

        if (StringUtils.isBlank(qualifier)) {
          scan.addFamily(Bytes.toBytes(family));
        }
        else {
          scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        }
      }
    }
    // specified column may or may not be part of first key value for the row.
    // Hence do not use FirstKeyOnlyFilter if scan has columns, instead use
    // FirstKeyValueMatchingQualifiersFilter.
    if (qualifiers.size() == 0) {
      scan.setFilter(new FirstKeyOnlyFilter());
    } else {
      scan.setFilter(new FirstKeyValueMatchingQualifiersFilter(qualifiers));
    }
    scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    job.setOutputFormatClass(NullOutputFormat.class); // 消耗所有输出并将它们放在/ dev / null中。
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
      RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  /*
   * @param errorMessage Can attach a message when error occurs.
   * @param errorMessage发生错误时可以附加一条消息。
   */
  private static void printUsage(String errorMessage) {
    System.err.println("ERROR: " + errorMessage);
    printUsage();
  }

  /*
   * Prints usage without error message
   * 为了提高性能，请考虑以下选项：-Dhbase.client.scanner.caching = 100 -Dmapreduce.map.speculative = false
   */
  private static void printUsage() {
    System.err.println("Usage: RowCounter [options] <tablename> " +
        "[--starttime=[start] --endtime=[end] " +
        "[--range=[startKey],[endKey]] [<column1> <column2>...]");
    System.err.println("For performance consider the following options:\n"
        + "-Dhbase.client.scanner.caching=100\n"
        + "-Dmapreduce.map.speculative=false");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 1) {
      printUsage("Wrong number of parameters: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    if (job == null) {
      System.exit(-1);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
