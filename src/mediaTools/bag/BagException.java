package mediaTools.bag;

import java.io.PrintWriter;
import java.io.StringWriter;

public class BagException extends Exception
{
  private static final long serialVersionUID = -8770566970130787799L;

  public BagException()
  {
  }

  public BagException(String arg0)
  {
    super(arg0);
  }

  public BagException(Throwable cause)
  {
    super(cause);

  }

  public BagException(String message, Throwable cause)
  {
    super(message, cause);
  }
  
  public String getStackTraceAsString()
  {
    StringWriter sw = new StringWriter();
    this.printStackTrace(new PrintWriter(sw));
    return sw.toString();    
  }
}
