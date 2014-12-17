package mediaTools.bag;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ChecksumException extends Exception
{
  private static final long serialVersionUID = -8770566970130787799L;

  public ChecksumException()
  {
  }

  public ChecksumException(String arg0)
  {
    super(arg0);
  }

  public ChecksumException(Throwable cause)
  {
    super(cause);

  }

  public ChecksumException(String message, Throwable cause)
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
