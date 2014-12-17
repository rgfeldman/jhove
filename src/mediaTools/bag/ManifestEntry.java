package mediaTools.bag;

public class ManifestEntry
{
  public String filename;
  public String checksum;

  public ManifestEntry(String filename, String checksum)
  {
    super();
    this.filename = filename;
    this.checksum = checksum;
  }
}
