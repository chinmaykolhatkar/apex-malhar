package com.datatorrent.schema;

/**
 * Created by chinmay on 14/4/16.
 */
public class TestPojo
{
  private String firstname;
  public String lastname;

  public TestPojo()
  {
    //for kryo
  }

  public TestPojo(String firstname, String lastname)
  {
    this.firstname = firstname;
    this.lastname = lastname;
  }

  public String getFirstname()
  {
    return firstname;
  }

  public void setFirstname(String firstname)
  {
    this.firstname = firstname;
  }
}
