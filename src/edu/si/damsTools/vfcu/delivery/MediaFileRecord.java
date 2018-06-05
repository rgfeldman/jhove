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
    private ArrayList<VfcuMd5File> assocMd5List;
    
    public MediaFileRecord () {
        vfcuMediaFile = new VfcuMediaFile();
    }
    
    public MediaFileRecord (Integer vfcuMediaFileId) {
        vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(vfcuMediaFileId);
        assocMd5List = new ArrayList<>();
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
    
    public boolean validateForCompletion(String hierarchyType) {
         //confirm 'PS' status exists
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            
            // if one member of a pair was an error, mark the other of the pair an error as well
            if(! hierarchyType.equals("master")) {
                //get the associated master, and check for error code 'ER' on it
            }
            else {
                //get the associated subfile, and check for error code 'ER' on it
            }
            
            //Check to make sure the PS status exists
            VfcuActivityLog vfcuActivityLog = new VfcuActivityLog();
            vfcuActivityLog.setVfcuMediaFileId(getVfcuMediaFile().getVfcuMediaFileId());
            vfcuActivityLog.setVfcuStatusCd("PS");
            boolean statusFound = vfcuActivityLog.doesMediaIdExistWithStatus();
            
            if (!statusFound ) {
                String errorCode = "VMS";
                if(! hierarchyType.equals("master")) {
                   errorCode = "VSM";
                }
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, errorCode, "Associated file not found");
                return false;
            }

        }
        
        return true;
    }
    
    public boolean populateBasicValuesFromDb() {
        vfcuMediaFile.populateBasicDbData();
        return true;
    }
        
    
    public boolean genAssociations(String currentFileHierarchy) {
  
        VfcuActivityLog activityLog = new VfcuActivityLog();
        activityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        activityLog.setVfcuStatusCd("PS");
        
        if (activityLog.doesMediaIdExistWithStatus()) {
            //no need to add association, we already added it
            return true;
        }
        
        HashMap<Integer,String> assocIds = new HashMap<>();
          
        //return associated records
        assocIds = vfcuMediaFile.returnAssocRecords();
                        
        for (Integer accocMediaId: assocIds.keySet()) {
            
            VfcuMediaFile assocVfcuMediaFile = new VfcuMediaFile();
            assocVfcuMediaFile.setVfcuMediaFileId(accocMediaId);
            
            if (currentFileHierarchy.equals("M") && assocIds.get(accocMediaId).equals("S") ) {         
                vfcuMediaFile.setChildVfcuMediaFileId(accocMediaId);
                vfcuMediaFile.updateChildVfcuMediaFileId();
            }
            else if (currentFileHierarchy.equals("S") && assocIds.get(accocMediaId).equals("M")) {
                assocVfcuMediaFile.setChildVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
                assocVfcuMediaFile.updateChildVfcuMediaFileId();
            }
            
            activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
            activityLog.setVfcuStatusCd("PS");
            activityLog.insertRow();
                
            activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(assocVfcuMediaFile.getVfcuMediaFileId());
            activityLog.setVfcuStatusCd("PS");
            activityLog.insertRow();
        }
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
            getVfcuMediaFile().setMediaFileSize(mediaFile.getMediaFileSize());
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
