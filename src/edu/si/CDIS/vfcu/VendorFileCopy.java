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
import java.security.MessageDigest;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import java.sql.Connection;
import edu.si.CDIS.vfcu.VendorMd5File;



/**
 *
 * @author rfeldman
 */
public class VendorFileCopy {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    
    String stagingForDAMS;
    
    private boolean loadMd5FileContents (File file) {
        //open file.  read into hash.  insert into database
        return true;
    }
    
    private boolean generateRecordNewMd5 () {
        //run checksum on file
        //record checksum
        return true;
    }
    
    private boolean validateMd5Value () {
        return true;
    }
    
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
                    
                boolean dataExtracted = md5File.extractDBInsertData (cdis.damsConn);
                if (! dataExtracted) {
                    continue;
                }    
                
            }
        }
        
        //End DB setup process
        
        
        
        //Get Files not yet assigned a batch yet, and create array of IDs for them
        // assign batch number on all of them
        
        //Step through array.
            //copy file to local area
            //generateRecordNewMd5 for the file;     
            //validateMd5Value vs whats in database ();
            //leave stub for jhove validation
            //record status as failed or successful
            //when all is done, mark batch status as done.

    }
}
