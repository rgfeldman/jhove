/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu;

import edu.si.CDIS.CDIS;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import edu.si.CDIS.vfcu.Database.VFCUMd5File;
import edu.si.CDIS.vfcu.Database.VFCUMediaFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.vfcu.VendorMd5File;
import java.util.Iterator;
import edu.si.CDIS.vfcu.MediaFile;
import edu.si.CDIS.vfcu.Database.VFCUActivityLog;
import edu.si.CDIS.vfcu.Database.VFCUError;


/**
 *
 * @author rfeldman
 */
public class VendorFileCopy {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String stagingForDAMS;
    
    
    /**
     *
     * @param cdis
     */
    public void validateAndCopy(CDIS cdis) {
    
        String vendorBatchLocation = null;
        
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentDate = df.format(new Date());
        
        //Get the location from the config file.  We typically have multiple file locations, one per filetype
        String[] vendorFileDirs = cdis.properties.getProperty("vendorBatchLocation").split(",");
        
        stagingForDAMS = cdis.properties.getProperty("vfcuStagingForCDIS");
        if (stagingForDAMS.contains("?DATE?")) {
            stagingForDAMS = stagingForDAMS.replaceAll("?DATE?", currentDate);
        }
        
        //for each vendorFileDirectory in config file, process the md5 file
        for (int i = 0; i < vendorFileDirs.length; i++) {
            
            vendorBatchLocation = vendorFileDirs[i];
            
            if (vendorBatchLocation.contains("?DATE?")) {
                vendorBatchLocation = vendorBatchLocation.replaceAll("?DATE?", currentDate);
            }
            
            // set variables for md5file 
            VendorMd5File md5File = new VendorMd5File();
            md5File.setVendorPath(vendorBatchLocation);
            md5File.setDamsStagingPath(stagingForDAMS);
            
            boolean fileLocated = md5File.locate (cdis.damsConn);
            
            if (! fileLocated) {
                logger.log(Level.FINEST, ".md5 File cannot be located in specified directory : " + vendorBatchLocation );
            }
            //see if this file is tracked in the database yet
            VFCUMd5File vfcuMd5File = new VFCUMd5File ();
            vfcuMd5File.setVendorFilePath(vendorBatchLocation);
            vfcuMd5File.setVendorMd5FileName(md5File.getFileName());
                        
            boolean md5FileInDB = vfcuMd5File.findExistingMd5File(cdis.damsConn);
            
            if (! md5FileInDB) {          
               
                //copy md5 file from vendor area.  If we are use the local copy of this file from here on
                boolean fileCopied = md5File.copyToDAMSStaging ();                            
                if (! fileCopied) {
                    //Get the next record in for loop
                    continue;
                }
                    
                //insert logging record
                boolean recordInserted = vfcuMd5File.insertRecord(cdis.damsConn);
                if (! recordInserted) {
                    continue;
                }
                    
                boolean dataExtracted = md5File.extractData (cdis.damsConn, vfcuMd5File);
                if (! dataExtracted) {
                    continue;
                }    
            }
        }
        
        //End DB setup process
        
        //Assign Files right away to a batch
        VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
        vfcuMediaFile.setMaxFiles(Integer.parseInt(cdis.properties.getProperty("vfcuMaxFilesBatch")));
        vfcuMediaFile.setVfcuBatchNumber(cdis.getBatchNumber());
        vfcuMediaFile.updateVfcuBatchNumber(cdis.damsConn);
        
        //Now we updated the files and assigned to current batch, commit so we lock them into current batch
        try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
        //create array of files just assigned to batch
        vfcuMediaFile.getFileIdsCurrentBatch(cdis.damsConn);
        
        for (Iterator<Integer> iter = vfcuMediaFile.getFilesIdsForBatch().iterator()  ; iter.hasNext();) {
            
            MediaFile mediaFile = new MediaFile();
            mediaFile.setVfcuMediaFileId(iter.next());
            mediaFile.setDamsStagingPath(stagingForDAMS);
            
            //get fileName, vendor_file_path for the current id
            mediaFile.populateFileNameAndPath(cdis.damsConn);
            
            mediaFile.copyToDamsStaging();

            //generateNewMd5 checksum for the file;     
            mediaFile.generateMd5Hash();
            
            //record checksum for the file; 
            vfcuMediaFile = new VFCUMediaFile();
            vfcuMediaFile.setVfcuMediaFileId(mediaFile.getVfcuMediaFileId());
            
            vfcuMediaFile.setVfcuChecksum(mediaFile.getVfcuMd5Hash());
            vfcuMediaFile.updateVfcuChecksum(cdis.damsConn);
            vfcuMediaFile.populateVendorChecksum(cdis.damsConn);
            
            VFCUActivityLog activityLog = new VFCUActivityLog();
            activityLog.setVfcuMediaFileId(mediaFile.getVfcuMediaFileId());
            
            //check to see if checksum values are the same from database
            if (vfcuMediaFile.getVendorChecksum().equals(vfcuMediaFile.getVendorChecksum()) ) {
                //log in the database
                activityLog.setVfcuStatusCd("VC");
            }
            else {
                activityLog.setVfcuStatusCd("ER");
                
                VFCUError vfcuError = new VFCUError();
                vfcuError.setVfcuMediaFileId(mediaFile.getVfcuMediaFileId());
                vfcuError.setVfcuErrorCd("MVV");
                vfcuError.insertRecord(cdis.damsConn);
            }    
        }
        
        
        //count the number of physical files and compare them to the number of physical files
        // in 
        
        //put CDIS_ready.txt file in directory
        //count the number of records that have not been assigned a batch for this date.
        // if the number of records = 0 then create a ready file
        createReadyFile();
        
    }

    private void createReadyFile () {
    
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = stagingForDAMS + "//CDIS_ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }
        
        
    
        
}
