/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.dimensions;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * This is a QueueManager for {@link DataQueryDimensional}.
 * </p>
 * <p>
 * <b>Note:</b> This {@link QueryExecutor} will work with {@link DimensionStoreHDHT}
 * operators that serve data for single or multiple schemas.
 * </p>
 */
public class DimensionsQueueManager extends AppDataWindowEndQueueManager<DataQueryDimensional, QueryMeta> {
  /**
   * The operator which stores the data.
   */
  @NotNull
  private final DimensionsStoreHDHT operator;
  /**
   * The schema registry from which to lookup {@link DimensionalSchema}s.
   */
  @NotNull
  private final SchemaRegistry schemaRegistry;

  /**
   * Creates a {@link DimensionsQueueManager} from the given {@link DimensionsStoreHDHT} and {@link SchemaRegistry}.
   * @param operator The {@link DimensionsStoreHDHT} to issue queries against.
   * @param schemaRegistry The {@link SchemaRegistry} which contains all the schemas served by the {@link DimensionsStoreHDHT}.
   */
  public DimensionsQueueManager(@NotNull DimensionsStoreHDHT operator, @NotNull SchemaRegistry schemaRegistry)
  {
    this.operator = Preconditions.checkNotNull(operator);
    this.schemaRegistry = Preconditions.checkNotNull(schemaRegistry);
  }

  @Override
  public boolean enqueue(DataQueryDimensional query, QueryMeta queryMeta, MutableLong windowExpireCount)
  {
    //Get the schema corresponding to this query.
    DimensionalSchema schemaDimensional = (DimensionalSchema) schemaRegistry.getSchema(query.getSchemaKeys());
    DimensionalConfigurationSchema configurationSchema = schemaDimensional.getDimensionalConfigurationSchema();
    Integer dimensionsDescriptorID = configurationSchema.getDimensionsDescriptorToID().get(query.getDimensionsDescriptor());

    if(dimensionsDescriptorID == null) {
      //Dimension combination not found
      LOG.debug("No aggregations for keys: {}", query.getKeyFields());
      return false;
    }

    //Create query key
    FieldsDescriptor keyDescriptor = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionsDescriptorID);
    GPOMutable gpoKey = query.createKeyGPO(keyDescriptor);
    Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();
    //The set of all incremental aggregations to query.
    Set<String> aggregatorNames = Sets.newHashSet();

    //loop through each different type of aggregation that is being queried.
    for(String aggregatorName: query.getFieldsAggregatable().getAggregators()) {
      if(!configurationSchema.getAggregatorRegistry().isAggregator(aggregatorName)) {
        //Check if a queried aggregation is valid.
        LOG.error(aggregatorName + " is not a valid aggregator.");
        return false;
      }

      if(configurationSchema.getAggregatorRegistry().isIncrementalAggregator(aggregatorName)) {
        //The incremental aggregations to query
        aggregatorNames.add(aggregatorName);
        continue;
      }

      //this is an OTF aggregator

      //gets the child aggregators of this otf aggregator and add it to the set of incremental aggregators to query
      aggregatorNames.addAll(configurationSchema.getAggregatorRegistry().getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
    }

    for(String aggregatorName: aggregatorNames) {
      //build the event key for each aggregator
      LOG.debug("querying for aggregator {}", aggregatorName);
      Integer aggregatorID = configurationSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(aggregatorName);
      EventKey eventKey = new EventKey(schemaDimensional.getSchemaID(), dimensionsDescriptorID, aggregatorID, gpoKey);
      //add the event key for each aggregator
      aggregatorToEventKey.put(aggregatorName, eventKey);
    }

    long bucketKey = operator.getBucketForSchema(schemaDimensional.getSchemaID());
    List<Map<String, EventKey>> eventKeys = Lists.newArrayList();
    List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();

    if(!query.isHasTime()) {
      //query doesn't have time

      //Create the queries
      Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
      Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

      for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
        //create the query for each event key

        String aggregatorName = entry.getKey();
        EventKey eventKey = entry.getValue();
        Slice key = new Slice(operator.getEventKeyBytesGAE(eventKey));
        //reuse the existing HDSQuery for the given key if it exists
        HDSQuery hdsQuery = operator.getQueries().get(key);

        if(hdsQuery == null) {
          //no prexisting query, so create a new one
          hdsQuery = new HDSQuery();
          hdsQuery.bucketKey = bucketKey;
          hdsQuery.key = key;
          operator.addQuery(hdsQuery);
        }
        else {
          //Work around for bug in HDS???
          if(hdsQuery.result == null) {
            hdsQuery.processed = false;
          }
        }

        //get the countdown for the query
        int countDown = (int)query.getCountdown();

        if(hdsQuery.keepAliveCount < countDown) {
          //keep alive time for shared query should be max countdown
          hdsQuery.keepAliveCount = countDown;
        }

        aggregatorToEventKeyMap.put(aggregatorName, eventKey);
        aggregatorToQueryMap.put(aggregatorName, hdsQuery);
      }

      hdsQueries.add(aggregatorToQueryMap);
      eventKeys.add(aggregatorToEventKeyMap);
    }
    else {
      //the query has time

      long endTime;
      long startTime;

      if(query.isFromTo()) {
        //If the query has from and to times

        //The from time in the query
        startTime = query.getTimeBucket().roundDown(query.getFrom());
        //the to time in the query
        endTime = query.getTimeBucket().roundDown(query.getTo());
      }
      else {
        //the query has lastnumbuckets

        long time = System.currentTimeMillis();
        endTime = query.getTimeBucket().roundDown(time);
        startTime = endTime - query.getTimeBucket().getTimeUnit().toMillis(query.getLatestNumBuckets() - 1);
      }

      gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, query.getTimeBucket().ordinal());

      //loop through each time to query
      for(long timestamp = startTime; timestamp <= endTime; timestamp += query.getTimeBucket().getTimeUnit().toMillis(1)) {
        Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
        Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

        //loop over aggregators
        for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
          String aggregatorName = entry.getKey();
          //create event key for this query
          EventKey eventKey = entry.getValue();
          gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);
          gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, query.getTimeBucket().ordinal());
          EventKey queryEventKey = new EventKey(eventKey);
          Slice key = new Slice(operator.getEventKeyBytesGAE(eventKey));
          //Check if there's an existing asynchronous query for this key
          HDSQuery hdsQuery = operator.getQueries().get(key);

          if(hdsQuery == null) {
            //If there is no existing HDSQuery create it
            hdsQuery = new HDSQuery();
            hdsQuery.bucketKey = bucketKey;
            hdsQuery.key = key;
            operator.addQuery(hdsQuery);
          }
          else {
            //Work around for bug in HDS???
            if(hdsQuery.result == null) {
              hdsQuery.processed = false;
            }
          }

          //get the countdown for the query
          int countDown = (int)query.getCountdown();

          if(hdsQuery.keepAliveCount < countDown) {
            //keep alive time for shared query should be max countdown
            hdsQuery.keepAliveCount = countDown;
          }

          aggregatorToEventKeyMap.put(aggregatorName, queryEventKey);
          aggregatorToQueryMap.put(aggregatorName, hdsQuery);
        }

        hdsQueries.add(aggregatorToQueryMap);
        eventKeys.add(aggregatorToEventKeyMap);
      }
    }

    //Create the query meta for the query
    QueryMeta qm = new QueryMeta();
    qm.setEventKeys(eventKeys);
    qm.setHdsQueries(hdsQueries);
    return super.enqueue(query, qm, null);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueueManager.class);
}
