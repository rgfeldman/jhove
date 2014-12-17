package mediaTools.bag;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import java.util.logging.Logger;


public class Bagger
{
  /**
   * 
   * @param logger
   * @param bagParentFolder
   * @param bagName
   * @param payload
   * @param removeOriginals
   * @throws BagException
   * @throws ChecksumException
   */
  public static void makeBag(Logger logger, File bagParentFolder, String bagName, File[] payload, boolean removeOriginals, int cdsAccessLevel) throws BagException, ChecksumException
  {
    // Create folder structure of the bag:
    
    File bagFolder = makeFolderStructure(logger, bagParentFolder, bagName);
    File dataFolder = new File(bagFolder.getAbsolutePath()+File.separator+"data");
    
    if ( payload == null )
    {
      throw new BagException("Invalid argument: payload cannot be null!"); 
    }
        
    HashMap<String,String> manifestData = new HashMap<String,String>();

    for (File file: payload)
    {
      // create a path relative to the root of the bag as required by the manifest: 
      String relativePath = "data"+File.separator+file.getName(); 

      File dest = new File(dataFolder.getAbsolutePath()+File.separator+file.getName());
      
      logger.info(" [Bagger.makeBag] Copy "+file.getAbsolutePath() +"  to  "+dest.getAbsolutePath());
      
      try
      {
        FileUtils.copyFile(file, dest);
      }
      catch (IOException ioe)
      {
        throw new BagException("FileUtils.copyFile threw IOException while copying "
            +file.getAbsolutePath()+" to "
            +dest.getAbsolutePath()+".\nIOException message: "+ioe.getMessage());
      }

      String originalChecksum = ChecksumUtils.getMD5ChecksumAsString(file);
      String destChecksum = ChecksumUtils.getMD5ChecksumAsString(dest);
      
      if ( originalChecksum.equals(destChecksum) )
      {
        manifestData.put(destChecksum, relativePath);
      }
      else
      {
        throw new BagException("Checksum mismatch on copy of "+file.getName()+"!\n"
            + "Orig: "+originalChecksum+"  "+file.getAbsolutePath()
            + "Dest: "+destChecksum+"  "+dest.getAbsolutePath());
      }
    }
    
    
    try
    {
      writeManifest(logger, bagFolder, manifestData);
    }
    catch (IOException ioe)
    {
      throw new BagException("IOException while writing manifest file!\nIOException message: "+ioe.getMessage());
    }

    // Only write the bag-info if there's a valid cds access level.
    // MetadataSync passes -1 in order to disable this feature (for now)
    
    if ( cdsAccessLevel > -1)
    {
      try
      {
        writeBagInfoTxt(logger, bagFolder, cdsAccessLevel);
      }
      catch (IOException ioe)
      {
        throw new BagException("IOException while writing bag-info.txt file!\nIOException message: "+ioe.getMessage());
      }
    }

    try
    {
      writeBagitFile(logger, bagFolder);
    }
    catch (IOException ioe)
    {
      throw new BagException("IOException while writing bagit file!\nIOException message: "+ioe.getMessage());
    }
    
    // If we've made it this far, then the bag was successfully created, and 
    // we're safely able to delete the originals
    
    if ( removeOriginals )
    {
      boolean deleted = false;
      for (File file: payload)
      {
        deleted = FileUtils.deleteQuietly(file);
        if ( ! deleted )
        {
          throw new BagException("Unable to delete input file: "+file.getAbsolutePath()+"\nBag successfully created, this error occurred during cleanup.");
        }
      }
    }
  }
  
  /**
   * 
   * @param logger
   * @param bagParentFolder
   * @param bagName
   * @return
   * @throws BagException
   */
  public static File makeFolderStructure(Logger logger, File bagParentFolder, String bagName) throws BagException
  {
    // Ensure the parent folder (where the bag will be created) 
    // exists and is writable:

    if ( bagParentFolder == null )
    {
      throw new BagException("Invalid argument: rootFolder cannot be null!"); 
    }
    if ( ! bagParentFolder.exists() )
      throw new BagException("The specified root folder '"+bagParentFolder.getAbsolutePath()+"' does not exist!");

    if ( ! bagParentFolder.canWrite() )
      throw new BagException("The specified root folder '"+bagParentFolder.getAbsolutePath()+"' is not writable!");
    
    logger.info(" [Bagger.makeBag] bagParentFolder = "+bagParentFolder.getAbsolutePath());

    // Validate the bag name and payload
    
    if ( bagName == null )
    {
      throw new BagException("Invalid argument: bagFolderName cannot be null!"); 
    }
    
    // Create the bag folder and data folder underneath

    File bagFolder = new File(bagParentFolder.getAbsolutePath() + File.separator + bagName);

    if (bagFolder.exists())
      throw new BagException("Bag folder "+bagName+"'' already exists!");
    
    File dataFolder = new File(bagFolder.getAbsolutePath()+File.separator+"data");
    
    try
    {
      FileUtils.forceMkdir(dataFolder);
    }
    catch (IOException ioe)
    {
    	ioe.printStackTrace();
      throw new BagException("IOException caught while attempting to create bag & data folders '"+bagFolder.getAbsolutePath()+"': "+ioe.getMessage());
    }

    logger.info(" [Bagger.makeBag] bagFolder = "+bagFolder.getAbsolutePath());
    return bagFolder;
  }



  /**
   * Alternate method, which does not require the boolean flag
   * 
   * @param rootFolder
   * @param bagFolderName
   * @param payload
   * @throws Exception
   */
  public static void makeBag(Logger logger, File rootFolder, String bagFolderName, File[] payload, int cdsAccessLevel) throws Exception
  {
    makeBag(logger, rootFolder, bagFolderName, payload, false, cdsAccessLevel);
  }

  
  /**
   * Creates a BagIt manifest from the specified HashMap
   * 
   * @param rootFolder    The folder where the manifest should be created
   * @param data          A HashMap of <checksum, filename> strings. 
   * @throws IOException
   */
  public static void writeManifest(Logger logger, File rootFolder, HashMap<String,String> data) throws IOException 
  {
    Vector<String> lines = new Vector<String>();
    
    String checksum = "";
    String filepath = "";
    Iterator<String> iter = data.keySet().iterator();
    while (iter.hasNext())
    {
      checksum = iter.next();
      filepath = data.get(checksum);

      // Sung's code is expecting forwardslash separators, 
      // not backslash, so switch it before writing:
      filepath = filepath.replaceAll("\\\\", "/");
      
      lines.add(checksum+" "+filepath);
    }
    
    File manifest = new File(rootFolder.getAbsolutePath()+File.separator+"manifest-md5.txt");
    logger.info(" [Bagger.makeBag] Writing manifest file: "+manifest.getAbsolutePath());
    FileUtils.writeLines(manifest, lines);
  }

  
  /**
   * Creates a standard bagit.txt file in the specified location
   * 
   * @param bagitFile
   * @param data
   * @throws IOException 
   */
  public static void writeBagitFile(Logger logger, File rootFolder) throws IOException
  {
    Vector<String> lines = new Vector<String>();
    lines.add("BagIt-Version: 0.96");
    lines.add("Tag-File-Character-Encoding: UTF-8");
    
    File bagit = new File(rootFolder.getAbsolutePath()+File.separator+"bagit.txt");
    logger.info(" [Bagger.makeBag] Writing bagit file: "+bagit.getAbsolutePath());
    FileUtils.writeLines(bagit, lines);
  }
  
  
  /**
   * 
   * @param logger
   * @param bagFolder
   * @param cdsAccessLevel
   * @throws IOException
   */
  public static void writeBagInfoTxt(Logger logger, File bagFolder, int cdsAccessLevel) throws IOException
  {
    Vector<String> lines = new Vector<String>();
    lines.add("cdsAccessLevel: "+cdsAccessLevel);
    
    File baginfo = new File(bagFolder.getAbsolutePath()+File.separator+"bag-info.txt");
    logger.info(" [Bagger.makeBag] Writing bag-info.txt file: "+baginfo.getAbsolutePath());
    FileUtils.writeLines(baginfo, lines);
  }
  
  
  /**
   * 
   * @param logger
   * @param rootFolder
   * @param fetchPath
   * @param dataPath
   * @throws IOException
   */
  public static void writeFetchTxt(Logger logger, File rootFolder, String fetchPath, String dataPath) throws IOException
  {
    Vector<String> lines = new Vector<String>();
    lines.add(fetchPath + " - " + dataPath);
    
    File f = new File(rootFolder.getAbsolutePath()+File.separator+"fetch.txt");
    logger.info(" [Bagger.writeFetchTxt] Writing fetch.txt: "+f.getAbsolutePath());
    FileUtils.writeLines(f, lines);
  }

}
