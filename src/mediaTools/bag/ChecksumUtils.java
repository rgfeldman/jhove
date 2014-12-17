package mediaTools.bag;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChecksumUtils
{
  private static final byte[] HEX_CHAR_TABLE = {
    (byte)'0', (byte)'1', (byte)'2', (byte)'3',
    (byte)'4', (byte)'5', (byte)'6', (byte)'7',
    (byte)'8', (byte)'9', (byte)'a', (byte)'b',
    (byte)'c', (byte)'d', (byte)'e', (byte)'f'
  };    


  /**
   * Returns a hex string representing the checksum of the file
   *  
   * @param file
   * @return
   * @throws Exception
   */
  public static String getMD5ChecksumAsString(File file) throws ChecksumException
  {
    return getHexString(createMD5Checksum(file));
  }

  /**
   * 
   * @param data
   * @return
   * @throws ChecksumException
   */
  public static String getMD5ChecksumAsString(String data) throws ChecksumException
  {
    return getHexString(createMD5Checksum(data));
  }
  
  
  /**
   * 
   * @param file
   * @return
   * @throws Exception
   */
  public static byte[] createMD5Checksum(File file) throws ChecksumException
  {
    try
    {
      InputStream fis;
      fis = new FileInputStream(file); // FileNotFoundException

      byte[] buffer = new byte[1024];
      MessageDigest complete = MessageDigest.getInstance("MD5"); // NoSuchAlgorithmException
      int numRead;
      do
      {
        numRead = fis.read(buffer); // IOException
        if (numRead > 0)
        {
          complete.update(buffer, 0, numRead);
        }
      }
      while (numRead != -1);
      fis.close(); // IOException
      
      return complete.digest();
    }
    catch (FileNotFoundException e)
    {
      throw new ChecksumException("FilNotFoundException from file stream read during checksum creation for "+file.getAbsolutePath()+"\nMessage: "+e.getMessage());
    }
    catch (NoSuchAlgorithmException e)
    {
      throw new ChecksumException("NoSuchAlgorithmException from getInstance(\"MD5\") during checksum creation for "+file.getAbsolutePath()+"\nMessage: "+e.getMessage());
    }
    catch (IOException e)
    {
      throw new ChecksumException("IOException from file stream read during checksum creation for "+file.getAbsolutePath()+"\nMessage: "+e.getMessage());
    }
  }
  
  
  /**
   * 
   * @param data
   * @return
   * @throws ChecksumException
   */
  public static byte[] createMD5Checksum(String data) throws ChecksumException
  {
    try
    {
      InputStream fis;
      fis = new ByteArrayInputStream(data.getBytes("ISO-8859-1"));

      byte[] buffer = new byte[1024];
      MessageDigest complete = MessageDigest.getInstance("MD5"); // NoSuchAlgorithmException
      int numRead;
      do
      {
        numRead = fis.read(buffer); // IOException
        if (numRead > 0)
        {
          complete.update(buffer, 0, numRead);
        }
      }
      while (numRead != -1);
      fis.close(); // IOException
      
      return complete.digest();
    }
    catch (NoSuchAlgorithmException e)
    {
      throw new ChecksumException("NoSuchAlgorithmException from getInstance(\"MD5\") during checksum creation for string.\nMessage: "+e.getMessage());
    }
    catch (IOException e)
    {
      throw new ChecksumException("IOException from byteArrayInputStream read during checksum creation for string.\nMessage: "+e.getMessage());
    }
  }
  
  /**
   * 
   * @param raw
   * @return
   * @throws UnsupportedEncodingException
   */
  public static String getHexString(byte[] raw) throws ChecksumException
  {
    byte[] hex = new byte[2 * raw.length];
    int index = 0;

    for (byte b : raw)
    {
      int v = b & 0xFF;
      hex[index++] = HEX_CHAR_TABLE[v >>> 4];
      hex[index++] = HEX_CHAR_TABLE[v & 0xF];
    }
    String output;
    try
    {
      output = new String(hex, "ASCII");
    }
    catch (UnsupportedEncodingException e)
    {
      throw new ChecksumException("UnsupportedEncodingException during getHextString\nMessage: "+e.getMessage());
    }
    return output;
  }
  
}
