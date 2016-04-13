package com.datatorrent.schema;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class RandomInputGenerator implements InputOperator
{
  private int maxEmitsPerWindow = 100;
  private int currentCount = 0;

  public final transient DefaultOutputPort<TestPojo> output = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {
    if (currentCount < maxEmitsPerWindow) {
      output.emit(new TestPojo("FirstName", "LastName"));
      currentCount++;
    }
  }

  @Override
  public void beginWindow(long l)
  {
    currentCount = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public int getMaxEmitsPerWindow()
  {
    return maxEmitsPerWindow;
  }

  public void setMaxEmitsPerWindow(int maxEmitsPerWindow)
  {
    this.maxEmitsPerWindow = maxEmitsPerWindow;
  }
}

