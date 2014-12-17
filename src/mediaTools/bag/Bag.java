package mediaTools.bag;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class Bag
{
  public static final int NORMAL = 0;
  public static final int XMLONLY = 1;
  public static final int HOLEY = 2;
  public static final int HOLEY_COMPLETE = 3;
  public static final int INCOMPLETE = 4;
  
  public static final String[] BAGTYPENAMES = {"NORMAL","XMLONLY","HOLEY","HOLEY_POPULATED","INCOMPLETE"};

  private File bagFolder;
  private String bagname;
  private int bagType;
  private File manifestFile;
  private File fetchFile;
  private File bagitFile;
  private File dataFolder;
  private Vector<ManifestEntry> manifest;
  private Vector<String> validationErrors = new Vector<String>();
  
  public Bag(File file) throws BagException
  {
    try
    {
      // Start with a folder which is assumed to be a bag (which may or may not be valid):
      this.bagFolder = file;
      
      // Parse data from the bag folder:
      
      this.bagname = bagFolder.getName();

      bagitFile = new File(this.bagFolder.getAbsolutePath()+File.separator+"bagit.txt");

      manifestFile = new File(this.bagFolder.getAbsolutePath()+File.separator+"manifest-md5.txt");
      if ( manifestFile.exists() )
      {
        try
        {
          this.manifest = loadManifest(manifestFile);
        }
        catch (Exception e)
        {
          // ignore in constructor
        }
      }

      fetchFile = new File(this.bagFolder.getAbsolutePath()+File.separator+"fetch.txt");
      
      dataFolder = new File(this.bagFolder.getAbsolutePath()+File.separator+"data");
    }
    catch (Exception e)
    {
      throw new BagException("Bag constructor caught exception:\n"+e.getMessage());
    }
  }
  
  /**
   * This validation method is predicated on both the BagIt specification
   * AND on the YUAG/YPM/YCBA bag specification.  In follows the atandard
   * BagIt validation rules EXCEPT for the HOLEY bagType.  
   * 
   * This is due to the network access restrictions on holey bag pointers
   * which are links into the rescue repository.  Basically what this 
   * business rule does is say that a HOLEY bag (where the 
   * asset has not been pre-fetched) is valid if everything but the fetch
   * pointer checks out.  I.e. it's essentially ignoring the fetch file.
   * 
   * 
   * 
   * 
   * @return
   * @throws ChecksumException 
   * @throws BagException 
   */
  public boolean isValid() throws ChecksumException, BagException
  {
    this.validationErrors = new Vector<String>();
    
    // If it's not a folder, or doesn't exist, bail out 
    // (note that bag validation happens later, this is the bare requirements)
    if ( ! this.bagFolder.exists() )
    {
      validationErrors.add("Bag folder "+bagFolder.getAbsolutePath()+" does not exist");
    }
    if ( ! this.bagFolder.isDirectory() )
    {
      validationErrors.add(bagFolder.getAbsolutePath()+" is not a directory!");
    }
    
    if ( ! bagitFile.exists() )
    {
      validationErrors.add("Bagit file does not exist");
    }
    
    if ( ! manifestFile.exists() )
    {
      validationErrors.add("Manifest file does not exist");
    }
    
    if ( manifest == null )
    {
      try
      {
        this.manifest = loadManifest(manifestFile);
      }
      catch (Exception e)
      {
        validationErrors.add(e.getClass()+" reading manifest file: "+e.getMessage());
      }
    }
    
    // Verify that all manifest items exist, and the manifest checksum matches the file:
    File f = null;
    for (ManifestEntry entry: manifest)
    {
      // Always check the xml file, regardless of holey/normal:
      if ( entry.filename.toLowerCase().endsWith(".xml") )
      {
        f = new File(this.bagFolder.getAbsolutePath()+File.separator+entry.filename);
        if ( ! f.exists() )
        {
          validationErrors.add("Manifest entry '"+entry.filename+"' does not exist");
        }
        else
        {
          String generatedChecksum = ChecksumUtils.getMD5ChecksumAsString(f);
          if ( ! entry.checksum.equals(generatedChecksum) )
          {
            validationErrors.add("Manifest checksum '"+entry.checksum+"' does not match the generated checksum of '"+generatedChecksum+"' on '"+entry.filename+"'.");
          }
        }
      }
      else
      {
        // If it's not an xml file, it must be the asset.
        // If this is a NORMAL/HOLEY_COMPLETE bag type, then validate the asset,
        // otherwise ignore it.
        if ( this.getBagType() != Bag.HOLEY )
        {
          f = new File(this.bagFolder.getAbsolutePath()+File.separator+entry.filename);
          if ( ! f.exists() )
          {
            validationErrors.add("Manifest entry '"+entry.filename+"' does not exist");
          }
          else
          {
            String generatedChecksum = ChecksumUtils.getMD5ChecksumAsString(f);
            if ( ! entry.checksum.equals(generatedChecksum) )
            {
              validationErrors.add("Manifest checksum '"+entry.checksum+"' does not match the generated checksum of '"+generatedChecksum+"' on '"+entry.filename+"'.");
            }
          }
        }
      }
    }
    
    if ( validationErrors.size() > 0) 
      return false;
    else
      return true;
  }
  
  
  /**
   * 
   * @param fetchFile
   * @return
   * @throws BagException
   */
  private String getFetchedAsset(File fetchFile) throws BagException
  {
    // fetch.txt (if it exists) contains:
    // http://gemini.library.yale.edu/yuag/YUAGCatalogue/Collection/Object/107/767/ag-obj-107767-001-rpd.tif - data/ag-obj-107767-001-rpd.tif

    String fetchedAssetPath = "";
    try
    {
      String line = "";
      LineIterator iter = FileUtils.lineIterator(fetchFile);
      while ( iter.hasNext() )
      {
        line = iter.nextLine();
        // http://gemini.li.../107/767/ag-obj-107767-001-rpd.tif - data/ag-obj-107767-001-rpd.tif
        fetchedAssetPath = this.bagFolder.getAbsolutePath()+File.separator+line.split("-")[1].trim();
      }
    }
    catch (IOException e)
    {
      throw new BagException("IOException while trying to read fetch file: "+fetchFile.getAbsolutePath());
    }
    return fetchedAssetPath;
  }


  /**
   * 
   * @param manifestFile
   * @return
   * @throws BagException
   */
  private Vector<ManifestEntry> loadManifest(File manifestFile) throws BagException
  {
    // manifest file - contains rows like:
    // 7a810deb67219894fbdc844b53d12a10 data/ag-obj-107767-001-rpd.tif
    // ccba7a44d15c6fb378fbf0cee79de03e data/ag-obj-107767-001-rpd.xml

    // Shouldn't ever get here, but just in case...
    if ( ! manifestFile.exists() )
    {
      throw new BagException("loadManifest: manifest file not found!");
    }
    
    Vector<ManifestEntry> data = new Vector<ManifestEntry>();
    String line = "";
    
    try
    {
      LineIterator iter = FileUtils.lineIterator(manifestFile);
      while ( iter.hasNext() )
      {
        line = iter.nextLine();
        data.add(new ManifestEntry(line.split(" ")[1], line.split(" ")[0]));
      }
    }
    catch (IOException e)
    {
      throw new BagException("IOException while trying to read bag manifest: "+manifestFile.getAbsolutePath());
    }
    
    return data;
  }

  /**
   * 
   * @return
   */
  public boolean isHoleyBag()
  {
    if ( this.fetchFile.exists() ) 
      return true;
    else 
      return false;
  }
  
  
  /**
   * Returns a file pointer to the non-xml manifest entry, but does not
   * check to ensure the file exists.  It could be a holeyBag entry, which
   * has not been downloaded yet, e.g..
   *  
   * @return
   * @throws BagException 
   */
  public File getAssetFile() throws BagException
  {
    try
    {
      if ( manifest == null )
      {
        loadManifest(manifestFile);
      }
    }
    catch (BagException e)
    {
      return null;
    }
    
    File asset = null;
    
    for (ManifestEntry entry: manifest)
    {
      if ( ! entry.filename.toLowerCase().endsWith(".xml") )
      {
        asset = new File(this.bagFolder.getAbsolutePath()+File.separator+entry.filename);
      }
    }
    return asset;
  }
  
  public String getAssetFilename()
  {
    String filename = null;
    try
    {
      File asset = getAssetFile();
      if ( asset != null ) filename = asset.getName();
    }
    catch (Exception e)
    {
      filename = "(couldn't retrieve name: "+e.getMessage()+")";
    }
    return filename;
  }

  /**
   * 
   * @return
   */
  public Vector<File> getPayload()
  {
    Vector<File> payload = new Vector<File>();
    for (ManifestEntry entry: manifest)
    {
      payload.add(new File(this.bagFolder.getAbsolutePath()+File.separator+entry.filename));
    }
    return payload;
  }
  
  /**
   * 
   * @return
   * @throws BagException
   */
  public Vector<String> getBagitText() throws BagException
  {
    return getFileContents(this.bagitFile, "Bagit");
  }

  /**
   * 
   * @return
   * @throws BagException
   */
  public Vector<String> getManifestText() throws BagException
  {
    return getFileContents(this.manifestFile,"Manifest");
  }
  
  /**
   * 
   * @return
   * @throws BagException
   */
  public Vector<String> getFetchText() throws BagException
  {
    return getFileContents(this.fetchFile,"Fetch");
  }

  /**
   * 
   * @param file
   * @return
   * @throws BagException
   */
  private Vector<String> getFileContents(File file, String filetype) throws BagException
  {
    if ( file == null )
    {
      throw new BagException(filetype + " file does not exist in this bag.");
    }
    
    Vector<String> contents = new Vector<String>();
    try
    {
      LineIterator iter = FileUtils.lineIterator(file);
      while ( iter.hasNext() )
      {
        contents.add(iter.nextLine());
      }
    }
    catch (IOException e)
    {
      throw new BagException("IOException while attempting to read "+file.getAbsolutePath()+"\nMessage: "+e.getMessage());
    }
    return contents;
  }
  
  /**
   * 
   * @return
   */
  public File getBagParentFolder()
  {
    return this.bagFolder.getParentFile();
  }
  
  /**
   * 
   * @return
   * @throws BagException
   */
  public int getBagType() throws BagException
  {
    // Determine the bag type:
    // There are basically four types of bags per our (YUAG/YCBA/YPM) specification:
    // 
    // (1) NORMAL     contains both an asset and xml file.  
    //
    // (2) XMLONLY    an update bag, handled as a metadata-only update 
    //
    // (3) HOLEY      a holey bag which has a fetch.txt in lieu of the asset.
    //
    // (4) HOLEY_COMPLETE   A holey bag which has been fully populated by the server.
    //                      Typically this is only the case when a holey bag is 
    //                      shunted into the /error folder
    //
    // (5) INCOMPLETE       data folder, bagit, or manifest missing       

    if ( ! dataFolder.exists() || ! bagitFile.exists() || ! manifestFile.exists() )
    {
      this.bagType = Bag.INCOMPLETE;
    }
    else if ( fetchFile.exists() )
    {
      // Does the file in fetch.txt exist in /data?
      File fetchedAsset = new File(getFetchedAsset(fetchFile));
      if ( fetchedAsset.exists() )
      {
        this.bagType = Bag.HOLEY_COMPLETE;
      }
      else
      {
        this.bagType = Bag.HOLEY;
      }
    }
    else
    {
      // normal or xml only
      if ( getAssetFile() != null )
      {
        this.bagType = Bag.NORMAL; 
      }
      else
      {
        this.bagType = Bag.XMLONLY;
      }
    }    
    
    return bagType;
  }
  
  /**
   * 
   * @return
   */
  public String getBagDate()
  {
    String date = "";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd kk:mm");
    date = sdf.format(new Date(bagFolder.lastModified()));
    return date;
  }
  
  /**
   * 
   * @return
   */
  public Vector<File> getFilesInDataFolder()
  {
    Vector<File> files = new Vector<File>();
    
    File[] filelist = dataFolder.listFiles();
    for (File f: filelist)
    {
      files.add(f);
    }

    return files;
  }
  
  /* Generated accessor methods below */
  public File getBagFolder()
  {
    return bagFolder;
  }
  public String getBagname()
  {
    return bagname;
  }
  public File getManifestFile()
  {
    return manifestFile;
  }
  public File getFetchFile()
  {
    return fetchFile;
  }
  public File getDataFolder()
  {
    return dataFolder;
  }
  public File getBagitFile()
  {
    return bagitFile;
  }
  public Vector<ManifestEntry> getManifest()
  {
    return manifest;
  }
  public Vector<String> getValidationErrors()
  {
    return validationErrors;
  }
}

