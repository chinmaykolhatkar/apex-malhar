package org.apache.apex.malhar.lib.window;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

public interface HeuristicWatermark<InputT> extends Component<Context.OperatorContext>
{
  ControlTuple.Watermark processTupleForWatermark(Tuple.WindowedTuple<InputT> input);
}
