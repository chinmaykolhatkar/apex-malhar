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

package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent;

/**
 * <p>
 * This aggregator creates an aggregate out of the first {@link InputEvent} encountered by this aggregator. All subsequent
 * {@link InputEvent}s are ignored.
 * </p>
 * <p>
 * <b>Note:</b> when aggregates are combined in a unifier it is not possible to tell which came first or last, so
 * one is picked arbitrarily to be the first.
 * </p>
 * @param <EVENT> The type of the input event.
 */
public class AggregatorFirst<EVENT> extends AbstractIncrementalAggregator<EVENT>
{
  private static final long serialVersionUID = 20154301646L;

  public AggregatorFirst()
  {
    //Do nothing
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_TYPE_MAP.get(inputType);
  }

  @Override
  public void aggregate(DimensionsEvent dest, EVENT src)
  {
    //Ignore
  }

  @Override
  public void aggregate(DimensionsEvent dest, DimensionsEvent src)
  {
    //Ignore
  }
}
