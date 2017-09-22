/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files;

import edu.si.damsTools.vfcu.database.VFCUActivityLog;
import edu.si.damsTools.vfcu.database.VFCUMd5File;
import edu.si.damsTools.vfcu.database.VFCUMediaFile;
import edu.si.damsTools.vfcu.utilities.ErrorLog;;

import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class VendorMd5File {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
   
    private String basePathStaging;
    private String basePathVendor;
    private String fileName;
    private String filePathEnding;
    private Integer md5FileId;
    private boolean masterIndicator;
    
    public String getBasePathStaging () {
        return this.basePathStaging;
    }
    
    public String getBasePathVendor () {
        return this.basePathVendor;
    }
    
    public String getFilePathEnding () {
        return this.filePathEnding;
    }
    
    public String getFileName () {
        return this.fileName;
    }
    
    public Integer getMd5FileId () {
        return this.md5FileId;
    }
    
    public boolean getMasterIndicator () {
        return this.masterIndicator;
    }
    
    
    public void setBasePathStaging (String basePathStaging) {
        this.basePathStaging = basePathStaging;
    }
    
    public void setBasePathVendor (String basePathVendor) {
        this.basePathVendor = basePathVendor;
    }
        
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
        
    public void setFilePathEnding (String filePathEnding) {
        this.filePathEnding = filePathEnding;
    }
  
    public void setMd5FileId (Integer md5FileId) {
         this.md5FileId = md5FileId;
    }
    
    public void setMasterIndicator (boolean masterIndicator) {
         this.masterIndicator = masterIndicator;
    }
        
    
    public boolean transferToDAMSStaging () {
        
        String vendorFileWithPath = getBasePathVendor() + "/" + getFilePathEnding() + "/" + getFileName ();
        String newDirectoryPath = getBasePathStaging() + "/" + getFilePathEnding();
        String stagingFileWithPath = newDirectoryPath + "/" + getFileName ();
        
        logger.log(Level.FINEST, "Create Dir: " + newDirectoryPath);
        
        logger.log(Level.FINEST, "Transfer From: " + vendorFileWithPath);
        logger.log(Level.FINEST, "Transfer To: " + stagingFileWithPath);
            
        try { 
            Path source      = Paths.get(vendorFileWithPath);
            Path destination = Paths.get(stagingFileWithPath);
            Path newDirecotry = Paths.get(newDirectoryPath);
            
            //Create the target directory
            Files.createDirectories(newDirecotry);
            
            if (DamsTools.getProperty("transferMd5File").equals("move") ) {
                Files.move(source, destination);
            }
            else {
                Files.copy(source, destination);
            }
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error in transfer of md5 file " + vendorFileWithPath +  " to: " + stagingFileWithPath, e);
            return false;
        }
        
        return true;
      
    }
    
    public boolean extractData (VFCUMd5File vfcuMd5File) {
        
        LineIterator lt = null;
        
        String fileWithPath = getBasePathStaging() + "/" + getFilePathEnding() + "/" + getFileName();
        
        try (FileInputStream fin= new FileInputStream(fileWithPath) ) {
            
            lt = IOUtils.lineIterator(fin, "utf-8");
            
            while(lt.hasNext()) {
                
                try {
                    String line=lt.nextLine();
                    //logger.log(Level.FINEST, "Line: " + line);
                    
                    String pattern = "(\\S+)\\s+(\\S.*)";
                    Pattern r = Pattern.compile(pattern);
                
                    Matcher m = r.matcher(line);
                
                    if (m.matches()) {
                        VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
                        
                        vfcuMediaFile.setVendorCheckSum(m.group(1).toLowerCase());
                        vfcuMediaFile.setMediaFileName(m.group(2).trim());
                        
                        //Skip the line if he filename is Thumbs.Db.  It is a temp windows generated file Do not even add it to Database
                        if (vfcuMediaFile.getMediaFileName().equals("Thumbs.db")) {
                            continue;
                        }
                    
                        logger.log(Level.FINEST, "Checksum: " + m.group(1));
                        
                        //skip any filenames that end with .md5 extension
                        if (vfcuMediaFile.getMediaFileName().endsWith(".md5")) {
                            continue;
                        }
                    
                        vfcuMediaFile.setVfcuMd5FileId(vfcuMd5File.getVfcuMd5FileId());
                
                        vfcuMediaFile.generateMediaFileId();
                        vfcuMediaFile.insertRow();
                
                        VFCUActivityLog vfcuActivityLog = new VFCUActivityLog();
                        vfcuActivityLog.setVfcuStatusCd("IV");
                        vfcuActivityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
                
                        vfcuActivityLog.insertRow();
                    }
                } catch (Exception e) {
                    logger.log(Level.FINEST, "Error reading line from md5 file " + fileWithPath + e);
                    return false;
                }
            }
        
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error reading md5 file " + fileWithPath + e);
            return false;
        } finally {
            lt.close();
        }
        
        return true;   
                  
    }
    
    public Integer recordInfoInDB () {
            
            //see if this file is tracked in the database yet
            VFCUMd5File vfcuMd5File = new VFCUMd5File ();
            vfcuMd5File.setBasePathVendor(getBasePathVendor());
            vfcuMd5File.setVendorMd5FileName(getFileName());
            vfcuMd5File.setBasePathStaging(getBasePathStaging());
            vfcuMd5File.setFilePathEnding(getFilePathEnding());
                        
            Integer md5FileId = vfcuMd5File.returnIdForNamePath();
            
            if  (md5FileId == null) {    
               
                logger.log(Level.FINEST, "md5Id not found in DB, need to add a new one: " + getFilePathEnding() + "/" + getFileName() );
                
                //get id sequence
                boolean idSequenceObtained = vfcuMd5File.generateVfcuMd5FileId();
                if (! idSequenceObtained) {
                    return -1;
                }
                
                //copy md5 file from vendor area.  we should use the local copy of this file from here on 
                //because we will have control of that file
                
                          
                boolean fileXfered = transferToDAMSStaging ();                            
                if (! fileXfered) {
                    //Get the next record in for loop
                    return -1;
                }
                
                //assign the masterID if needed. in some cases we leave it as null and assign this value later
                if (DamsTools.getProperty("useMasterSubPairs").equals("true") ) {
                    if (filePathEnding.endsWith(DamsTools.getProperty("vendorMasterFileDir")) ) {
                    //Set the masterMd5ID for the master only, for subfiles it is assigned later
                        vfcuMd5File.setMasterMd5FileId(vfcuMd5File.getVfcuMd5FileId());
                    }
                }
                else {
                    // if there are no master/subfile pairs...just assign it as a master
                    vfcuMd5File.setMasterMd5FileId(vfcuMd5File.getVfcuMd5FileId());
                }
                
                //insert logging record
                boolean recordInserted = vfcuMd5File.insertRecord();
                if (! recordInserted) {
                    return -1;
                }
                    
                boolean dataExtracted = extractData (vfcuMd5File);
                if (! dataExtracted) {
                    return -1;
                }        
            }
            else if (md5FileId < 0) {
                logger.log(Level.FINEST, "Error, unable to find md5FileId");
                return -1;
            }
            else {
                //We found the masterID already in the database
                vfcuMd5File.setVfcuMd5FileId(md5FileId);
                logger.log(Level.FINEST, "md5Id found in DB, skipping: " + getFilePathEnding() + "/" + getFileName() );
            }
            
            return vfcuMd5File.getVfcuMd5FileId();
            
    }
    
    
    public boolean locateVendorAndPopulateName () {
        
        String vendorLocation = getBasePathVendor() + "/" + getFilePathEnding(); 
        File dirLocation = new File(vendorLocation);
        
        if(! dirLocation.isDirectory()){ 
            //get directory listing of all files in the directory,
            logger.log(Level.FINEST, "Error, unable to locate Vendor Location Directory: " + vendorLocation);
            return false;
        }
        
        File[] listOfFiles = dirLocation.listFiles(); 
        for (File file : listOfFiles) {
            if (file.toString().endsWith(".md5")) {
                                
                // if we have an md5 file at the location, save the info 
                setFileName (file.getName()); 
                
                logger.log(Level.FINEST, "Found md5 file: " + getFileName());
                
                return true;
            }    
        }       
        
        //we have not found any .md5 File in the directory
        logger.log(Level.FINEST, "Unable to find md5 file: ");
        return false;
        
    }
    
    public boolean finalValidations () {
        //Get counts----
        
        VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
        vfcuMediaFile.setVfcuMd5FileId(getMd5FileId());
        
        //First get the total count of files in Database for the current md5 ID
        int totalNumMediaFiles = vfcuMediaFile.returnCountFilesForMd5Id();
        if (! (totalNumMediaFiles > 0)) {
            logger.log(Level.FINEST, "unable to find any media Files for current md5: " + getMd5FileId());
            return false;
        }
        
        //Then get the number of files in the database that have PM validation with no errors
        int numPMVldt = vfcuMediaFile.returnCountStatusNoErrorForMd5Id("PM");
        if (! (numPMVldt > 0)) {
            logger.log(Level.FINEST, "unable to find any media Files with PM validation: " + getMd5FileId());
            return false;
        }
        
        //Then get the number of files in the database that have errors
        int numErrorRecords = vfcuMediaFile.returnCountErrorForMd5Id();
        
        int numFilesToProcess =  totalNumMediaFiles - (numPMVldt + numErrorRecords) ; 
        if ( numFilesToProcess > 0 ) {
             logger.log(Level.FINEST, "Still have " + numFilesToProcess + " files to process, will not take next step: " + getMd5FileId());
             return false;
        }
        
        //We do an additional step if the config file specifies to check that all the files in the directory were actually in the md5 file
        if (DamsTools.getProperty("VldtAllMediaXfered").equals("true")) {
            
            VFCUMd5File vfcuMd5File = new VFCUMd5File();
            vfcuMd5File.setVfcuMd5FileId(getMd5FileId());
            vfcuMd5File.populateVendorFilePath();
            
            ArrayList <String> mediaFilesFromDir = returnMediaFileListDir(vfcuMd5File.getBasePathVendor() + "/" + vfcuMd5File.getFilePathEnding());
            
            if (!(numPMVldt < mediaFilesFromDir.size())) {
                //Flag as error, we have some files still on the physical drive that were not in the md5 file.
                
                for (String fileName : mediaFilesFromDir) {
                    
                    vfcuMediaFile = new VFCUMediaFile();
                    
                    //check if the file from the directory exists in the database
                    vfcuMediaFile.setMediaFileName(fileName);
                    vfcuMediaFile.setVfcuMd5FileId(getMd5FileId());
                    boolean idRetrieved = vfcuMediaFile.populateMediaFileIdForFileNameMd5Id();
                    
                    if (! idRetrieved) {
                        //we couldnt find the current file name in the current md5 file, put in error
                        
                        ErrorLog errorLog = new ErrorLog(); 
                        errorLog.capture(vfcuMediaFile, "NVF", "Error, number of files in vendor dir != num files in db report");
                    }
                }   
            }
        }
        
        return true;
        
    }
    
    private ArrayList returnMediaFileListDir(String path) {
        
        ArrayList <String> mediaFilesFromDir;
        mediaFilesFromDir = new ArrayList <>();
        
        try {
            File dirLocation = new File(path);
        
            if(! dirLocation.isDirectory()){ 
                //get directory listing of all files in the directory,
                logger.log(Level.FINEST, "Error, unable to locate Location Directory: " + path);
            }
        
            File[] listOfFiles = dirLocation.listFiles(); 
            for (File file : listOfFiles) {
                String fileNameExt = FilenameUtils.getExtension(file.getName());
                if (! (fileNameExt.equalsIgnoreCase("md5") )) {
                    mediaFilesFromDir.add(file.getName());
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain count of files from directory", e );
        }
           
        return mediaFilesFromDir;
    }
}

