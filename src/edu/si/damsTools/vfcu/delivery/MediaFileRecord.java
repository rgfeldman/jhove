/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.utilities.ErrorLog;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.util.HashMap;
import java.util.logging.Level;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.delivery.files.MediaFile;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import java.nio.file.Path;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class MediaFileRecord {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final VfcuMediaFile vfcuMediaFile;
    
    public MediaFileRecord () {
        vfcuMediaFile = new VfcuMediaFile();
    }
    
    public MediaFileRecord (Integer vfcuMediaFileId) {
        vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(vfcuMediaFileId);
    }
    
    public VfcuMediaFile getVfcuMediaFile() {
        return this.vfcuMediaFile;
    }
     
    public boolean validateDbRecord() {
        //validate the filename (duplicate check)
        if (DamsTools.getProperty("dupFileNmCheck").equals("true")) {
            //look to see if the file already exists that is not in error state
            Integer otherFileName = vfcuMediaFile.returnIdForNameOtherMd5();
            if (otherFileName != null) {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "DUP", "Error: Duplicate File Found");
                return false;
            }
        }
        
        //check to see if checksum values are the same from database
        if (vfcuMediaFile.getVendorChecksum().equals(vfcuMediaFile.getVfcuChecksum())) {
            //log in the database
            VfcuActivityLog activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
            activityLog.setVfcuStatusCd("PM");
            activityLog.insertRow();
        }
        else {
            ErrorLog errorLog = new ErrorLog();  
            errorLog.capture(vfcuMediaFile, "VMD", "MD5 checksum validation failure");
            return false;
        }           
        
        return true;
    }
    
    public boolean checkAssociatedFiles(String hierarchyType) {
        
        logger.log(Level.FINEST,"Validating: " + hierarchyType); 

        populateBasicValuesFromDb();
        
        //Look to see if the associated file is in error, if it is then mark the current one as an error as well.        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
        
            // if one member of a pair was an error, mark the other of the pair an error as well
            if(hierarchyType.equals("master")) {
                //get the associated child, and check for error code 'ER' on it
                VfcuActivityLog subfileActivityLog = new VfcuActivityLog();
                subfileActivityLog.setVfcuMediaFileId(vfcuMediaFile.getChildVfcuMediaFileId());
                subfileActivityLog.setVfcuStatusCd("ER");
                boolean subfileInError = subfileActivityLog.doesMediaIdExistWithStatus();
                if (subfileInError) {
                    //Mark the master as an error as well
                    ErrorLog errorLog = new ErrorLog();  
                    errorLog.capture(vfcuMediaFile, "CFF", "Associated child File failed");
                    return false;
                }
            }
            else {
                //we are looking at subfile, get the associated master, and check for error code 'ER' on it
                VfcuActivityLog masterfileActivityLog = new VfcuActivityLog();
                masterfileActivityLog.setVfcuMediaFileId(vfcuMediaFile.retrieveParentVfcuMediafileId());
                if (masterfileActivityLog.getVfcuMediaFileId() == null) {
                    ErrorLog errorLog = new ErrorLog();  
                    errorLog.capture(vfcuMediaFile, "VSM", "Associated file not found");
                    return false;
                }
                masterfileActivityLog.setVfcuStatusCd("ER");
                boolean masterfileInError = masterfileActivityLog.doesMediaIdExistWithStatus();
                if (masterfileInError) {
                    //Mark the master as an error as well
                    ErrorLog errorLog = new ErrorLog();  
                    errorLog.capture(vfcuMediaFile, "MFF", "Associated master File failed");
                    return false;
                }
 
            }           
        }
        
        return true;
    }
    
    public boolean populateBasicValuesFromDb() {
        vfcuMediaFile.populateBasicDbData();
        return true;
    }
    
    
    public boolean validateAndTransfer(XferType xferType) {
        
        SourceFileListing sourceFileListing = new SourceFileListing(getVfcuMediaFile().getVfcuMd5FileId());
        sourceFileListing.populateBasicValuesFromDbVendor();
            
        //Get the filename and path based on the path from the md5File
        Path pathFile = sourceFileListing.getMd5File().getFileNameWithPath().resolveSibling(getVfcuMediaFile().getMediaFileName());      
        logger.log(Level.FINER, "fileLoc:" + pathFile.toString());

            MediaFile mediaFile = new MediaFile(pathFile);
            
            boolean mediaTransfered = mediaFile.transferToVfcuStaging(xferType, false);   
            
            if (!mediaTransfered) {
                ErrorLog errorLog = new ErrorLog(); 
                errorLog.capture(getVfcuMediaFile(), xferType.returnXferErrorCode(), "Failure to xfer Vendor File");        
                return false;
            }
                
            //Insert the code indicating that the file was just transferred
            VfcuActivityLog activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(getVfcuMediaFile().getVfcuMediaFileId());
            activityLog.setVfcuStatusCd(xferType.returnCompleteXferCode());
            activityLog.insertRow();
                
            //Populate attributes post-file transfer
            boolean attributesGathered = mediaFile.populateAttributes();
            if (!attributesGathered) {
                ErrorLog errorLog = new ErrorLog(); 
                errorLog.capture(getVfcuMediaFile(), xferType.returnXferErrorCode(), "Attribute gathering failed");        
                return false;
            }
            getVfcuMediaFile().setVfcuChecksum(mediaFile.getMd5Hash());
            getVfcuMediaFile().setMbFileSize(mediaFile.getMbFileSize());
            getVfcuMediaFile().setMediaFileDate(mediaFile.getMediaFileDate());
            getVfcuMediaFile().updateVfcuMediaAttributes();           
            
            //Perform validations on the physical files
            String errorCode = mediaFile.validate();
            if (errorCode != null) {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(getVfcuMediaFile(), errorCode, "Validation Failure");
                return false;
            }
            
            //If we validated with jhove, now we need to record this in the datbase
            if (mediaFile.retJhoveValidated()) {
                activityLog.setVfcuMediaFileId(getVfcuMediaFile().getVfcuMediaFileId());
                activityLog.setVfcuStatusCd("JH");
                activityLog.insertRow();
            }
            
            //Perform validations on the database Record
            validateDbRecord();
            
        return true;
    }
}
