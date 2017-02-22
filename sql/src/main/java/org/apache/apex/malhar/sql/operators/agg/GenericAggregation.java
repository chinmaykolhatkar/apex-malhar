package org.apache.apex.malhar.sql.operators.agg;

import com.datatorrent.lib.expression.Expression;
import com.datatorrent.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.window.Accumulation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GenericAggregation<InputT, OutputT>
    implements Accumulation<InputT, Map<Object, List<InputT>>, List<OutputT>>
{
  private String keyExpression;
  private PojoUtils.Getter keyExtractor;
  private Aggregator aggregator;

  public GenericAggregation(String keyExpression, Aggregator agg)
  {
    this.keyExpression = keyExpression;
    this.aggregator = agg;
  }

  private Object extractKey(InputT inp)
  {
    if (keyExtractor == null) {
      keyExtractor = PojoUtils.createGetter(inp.getClass(), keyExpression, Object.class);
    }

    return keyExtractor.get(inp);
  }

  @Override
  public Map<Object, List<InputT>> defaultAccumulatedValue()
  {
    return new HashMap<>();
  }

  @Override
  public Map<Object, List<InputT>> accumulate(Map<Object, List<InputT>> accumulatedValue, InputT input)
  {
    Object key = extractKey(input);
    List<InputT> accList;
    if (accumulatedValue.containsKey(key)) {
      accList = accumulatedValue.get(key);
    }
    else {
      accList = new LinkedList<>();
      accumulatedValue.put(key, accList);
    }

    accList.add(input);

    return accumulatedValue;
  }

  @Override
  public Map<Object, List<InputT>> merge(Map<Object, List<InputT>> accumulatedValue1, Map<Object, List<InputT>> accumulatedValue2)
  {
    for (Map.Entry<Object, List<InputT>> entry : accumulatedValue1.entrySet()) {
      Object key = entry.getKey();

      if (accumulatedValue2.containsKey(key)) {
        accumulatedValue2.get(key).addAll(entry.getValue());
      }
      else {
        accumulatedValue2.put(key, entry.getValue());
      }
    }

    return accumulatedValue2;
  }

  @Override
  public List<OutputT> getOutput(Map<Object, List<InputT>> accumulatedValue)
  {
    List<OutputT> out = new LinkedList<>();
    for (List<InputT> inputTS : accumulatedValue.values()) {
      OutputT compute = (OutputT) aggregator.compute(inputTS);
      out.add(compute);
    }

    return out;
  }

  @Override
  public List<OutputT> getRetraction(List<OutputT> value)
  {
    return null;
  }

  public interface Aggregator<InputT, OutputT>
  {
    OutputT compute(List<InputT> inpList);
  }
}
