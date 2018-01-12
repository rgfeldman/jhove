/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database.dbRecords;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.utilities.ErrorLog;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.util.HashMap;
import java.util.logging.Level;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
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
     
    public boolean validate() {
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
    
    public boolean populateBasicValuesFromDb() {
        vfcuMediaFile.populateBasicDbData();
        return true;
    }
        
    
    public boolean genAssociations(String currentFileHierarchy) {
        HashMap<Integer,String> assocIds = new HashMap<>();
                               
        for (VfcuMd5File assocMd5File : assocMd5List) {
            //return comparable record for the Md5FileID provided
            assocIds = vfcuMediaFile.returnAssocRecords();
        }
                        
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
            
            VfcuActivityLog activityLog = new VfcuActivityLog();
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
}
