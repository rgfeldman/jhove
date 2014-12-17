/**
 CDIS 2.0 - Common Code
 HoleyBagData.java
 */
package edu.si.dams;

class HoleyBagData {
    
  public int id;
  public String rrpath;
  public String filename;
  public String checksum;
  public String url;
  public String xmlFilename;
  public HoleyBagData(int id, String rrpath, String filename, String checksum,
      String url, String xmlFilename)
  {
    super();
    this.id = id;
    this.rrpath = rrpath;
    this.filename = filename;
    this.checksum = checksum;
    this.url = url;
    this.xmlFilename = xmlFilename;
  }
    
}
