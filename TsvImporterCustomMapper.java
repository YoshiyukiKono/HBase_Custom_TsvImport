/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.InvalidLabelException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.net.URI;

// KONO

/**
 * Write table content out to map output files.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TsvImporterCustomMapper
//extends TsvImporterMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
{

  /** Timestamp for all inserted rows */
  protected long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  protected ImportTsv.TsvParser parser;

  protected Configuration conf;

  protected String cellVisibilityExpr;

  protected long ttl;

  protected CellCreator kvCreator;

  private String hfileOutPath;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  String columnsContents = null;
  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   * Common initialization that might be leveraged by a subsclass is done in
   * <code>doSetup</code>. Hence a subclass may choose to override this method
   * and call <code>doSetup</code> as well before handling it's own custom params.
   *
   * @param context
   */
  @Override
  protected void setup(Context context) {
    System.err.println("[TsvImporterCustomMapper] setup !!!!!!!!!!!!!!!!!!!!!!!!!");
    System.out.println("[TsvImporterCustomMapper] setup !!!!!!!!!!!!!!!!!!!!!!!!!");
    doSetup(context);

    Configuration conf = context.getConfiguration();

    // KONO
    //setupDistributedCache(context);

    // KONO START
    //if(StringUtils.isNotEmpty(conf.get(ImportTsv.COLUMNS_LIST_CONF_KEY))) {
    //  String columnsFilePath = conf.get(ImportTsv.COLUMNS_LIST_CONF_KEY);
    //  try {
    //    String columnSpecification = ImportTsv.readColumnSpecification(columnsFilePath);
    //   parser = new ImportTsv.TsvParser(columnSpecification, separator);
    //  } catch (IOException ex) {
    //    throw new RuntimeException("[KONO CDH] Can NOT read Columns List File [" + columnsFilePath + "] " + ex.getLocalizedMessage());
    //  }
    //} else { // KONO
    //  parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY), separator);
    //} // KONO END
    //String
    columnsContents = getColumnsContentsFromLocal(context);
    parser = new ImportTsv.TsvParser(columnsContents, separator);
    // [OUT BY KONO] parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY), separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
    this.kvCreator = new CellCreator(conf);
  }

  String localPath = null;

  // KONO START
  private String getColumnsContentsFromLocal(Context context) {
    Configuration conf = context.getConfiguration();
    try {
      org.apache.hadoop.fs.Path[] localPaths = context.getLocalCacheFiles();
      localPath =  localPaths[0].toString();
      String contentsFromUri = FileUtils.readFileToString(new java.io.File(localPaths[0].toString()));
      return contentsFromUri;
    } catch (Exception ex) {
      System.err.println("[FAILED] getColumnsContentsFromLocal");
      throw new RuntimeException("[FAILED] getColumnsContentsFromLocal", ex);
    }
  }
  // KONO START
  private String getColumnsContents(Context context) throws IOException {
    Configuration conf = context.getConfiguration();

    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
      URI cacheFileUri = context.getCacheFiles()[0];

      String columnContents = null;
      if (cacheFileUri != null) {
        // Cacheファイルが見つかったら、Stringにしてstdoutに出力
        System.err.println("Cache File URI: " + cacheFileUri);
        String contentsFromUri = FileUtils.readFileToString(new java.io.File(cacheFileUri));

        System.err.println("Contents Of Cache File: " + FileUtils.readFileToString(new java.io.File(cacheFileUri)));
        if (StringUtils.isNotEmpty(contentsFromUri)) {
          columnContents = contentsFromUri;
        }

        System.err.println("Cache File: " + ImportTsv.COLUMNS_LIST_CONF_KEY);
        String contentsFromListKey = FileUtils.readFileToString(new java.io.File(ImportTsv.COLUMNS_LIST_CONF_KEY));
        System.err.println("Contents Of Cache File: " + FileUtils.readFileToString(new java.io.File(ImportTsv.COLUMNS_LIST_CONF_KEY)));
        if (StringUtils.isNotEmpty(contentsFromListKey)) {
          columnContents = contentsFromListKey;
        }
        if (StringUtils.isNotEmpty(columnContents)) {
          return columnContents;
        } else {
          System.err.println("NO COLUMN CONTENTS");
          return conf.get(ImportTsv.COLUMNS_CONF_KEY);
        }

      } else {
        System.err.println("NO CACHE FILE");
        return conf.get(ImportTsv.COLUMNS_CONF_KEY);
      }
    } else {
      System.err.println("NO CACHE FILES AT ALL");
      return conf.get(ImportTsv.COLUMNS_CONF_KEY);
    }
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
    if (separator == null) {
      separator = ImportTsv.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }
    // Should never get 0 as we are setting this to a valid value in job
    // configuration.
    ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, 0);

    skipBadLines = context.getConfiguration().getBoolean(
            ImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
    hfileOutPath = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
  }

  // KONO END
  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value,
                  Context context)
          throws IOException {
    byte[] lineBytes = value.getBytes();

    try {
      ImportTsv.TsvParser.ParsedLine parsed = parser.parse(
              lineBytes, value.getLength());
      ImmutableBytesWritable rowKey =
              new ImmutableBytesWritable(lineBytes,
                      parsed.getRowKeyOffset(),
                      parsed.getRowKeyLength());
      // Retrieve timestamp if exists
      ts = parsed.getTimestamp(ts);
      cellVisibilityExpr = parsed.getCellVisibility();
      ttl = parsed.getCellTTL();

      Put put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()
                || i == parser.getAttributesKeyColumnIndex() || i == parser.getCellVisibilityColumnIndex()
                || i == parser.getCellTTLColumnIndex()) {
          continue;
        }
        populatePut(lineBytes, parsed, put, i);
      }
      context.write(rowKey, put);
    } catch (InvalidLabelException badLine) {
      if (skipBadLines) {
        System.err.println(
                "Bad line at offset: " + offset.get() + ":\n" +
                        badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (ImportTsv.TsvParser.BadTsvLineException badLine) {
      if (skipBadLines) {
        System.err.println(
                "Bad line at offset: " + offset.get() + ":\n" +
                        badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (IllegalArgumentException e) {
      if (skipBadLines) {
        System.err.println(
                "Bad line at offset: " + offset.get() + ":\n" +
                        e.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(e);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  protected void populatePut(byte[] lineBytes, ImportTsv.TsvParser.ParsedLine parsed, Put put,
                             int i) throws BadTsvLineException, IOException {
    Cell cell = null;
    if (hfileOutPath == null) {
      cell = new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
              parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
              parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes,
              parsed.getColumnOffset(i), parsed.getColumnLength(i));
      if (cellVisibilityExpr != null) {
        // We won't be validating the expression here. The Visibility CP will do
        // the validation
        put.setCellVisibility(new CellVisibility(cellVisibilityExpr));
      }
      if (ttl > 0) {
        put.setTTL(ttl);
      }
    } else {
      // Creating the KV which needs to be directly written to HFiles. Using the Facade
      // KVCreator for creation of kvs.
      List<Tag> tags = new ArrayList<Tag>();
      if (cellVisibilityExpr != null) {
        tags.addAll(kvCreator.getVisibilityExpressionResolver()
                .createVisibilityExpTags(cellVisibilityExpr));
      }
      // Add TTL directly to the KV so we can vary them when packing more than one KV
      // into puts
      if (ttl > 0) {
        tags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
      }
      try {
        cell = this.kvCreator.create(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
                parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
                parser.getQualifier(i).length, ts, lineBytes, parsed.getColumnOffset(i),
                parsed.getColumnLength(i), tags);
      } catch (Exception ex) {
        String msg = "localPath:[" + this.localPath + "] - columnsContents:[" + this.columnsContents
                +"] parser:[" + parser + "] parsed:[" + parsed + "] kvCreator:[" + this.kvCreator + "] ex:" + ex.toString();
        ex.printStackTrace();
        throw new RuntimeException(msg, ex);
      }
    }
    put.add(cell);
  }


}
