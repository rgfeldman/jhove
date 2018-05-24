/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import edu.si.damsTools.vfcu.delivery.files.Md5File;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.nio.file.Path;
/**
 *
 * @author rfeldman
 * Class: SourceFileListing
 * Description: This Class holds the relevant information pertaining to a listing of files provided by the 
 *              SI_unit or Vendor.  The Class represents a wrapper around the md5 File record in the database
 *              along with the class representing the physical file object itself.
 */
public class SourceFileListing {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final VfcuMd5File vfcuMd5File;
    private Md5File md5File;
    
    public SourceFileListing () {
        vfcuMd5File = new VfcuMd5File();
    }
    
    public SourceFileListing (Integer vfcuMd5FileId) {
        vfcuMd5File = new VfcuMd5File();
        vfcuMd5File.setVfcuMd5FileId(vfcuMd5FileId);
    }
    
    public VfcuMd5File getVfcuMd5File() {
        return this.vfcuMd5File;
    }
    
    public Md5File getMd5File() {
        return this.md5File;
    }
    
    public String returnStringBatchDir() {
        String dirName = vfcuMd5File.getBasePathVendor() + "/" + vfcuMd5File.getFilePathEnding();
        
        //In case there is no filePathEnding we will have a '/' at the end.  get rid of the final '/'
        dirName = dirName.replaceAll("/$", "");
        
        return dirName;
    }
    
    public boolean checkIfExistsInDb () {
            
        //see if this file is tracked in the database yet                    
        Integer md5FileId = vfcuMd5File.returnIdForNamePath();
           
        if  (md5FileId == null) {         
            logger.log(Level.FINEST, "md5Id not found in DB, " +  vfcuMd5File.getFilePathEnding() );
            return false;
        }    
        return true;          
    }
    
    
//    public boolean populateBasicValuesFromDeliveryFile (String fileName, String filePathEnding) {
    public boolean populateBasicValuesFromDeliveryFile (Path nameAndPath) {


        md5File = new Md5File(nameAndPath);

        vfcuMd5File.setVendorMd5FileName(md5File.getFileName());
        vfcuMd5File.setFilePathEnding(md5File.getLocalPathEnding());
            
        vfcuMd5File.setBasePathStaging(DamsTools.getProperty("vfcuStaging"));  
        vfcuMd5File.setBasePathVendor(DamsTools.getProperty("sourceBaseDir"));
            
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
                    
            if (vfcuMd5File.getFilePathEnding().endsWith(DamsTools.getProperty("vendorMasterFileDir"))) {
                vfcuMd5File.setFileHierarchyCd("M");
            }
            else if (vfcuMd5File.getFilePathEnding().endsWith(DamsTools.getProperty("vendorSubFileDir"))) {
                vfcuMd5File.setFileHierarchyCd("S");
            }
        }
        else {
            vfcuMd5File.setFileHierarchyCd("M");
        }

        return true;
    }
    
    public boolean retrieveAndRecord(XferType xferType) {

        // Check to see if md5 file already exists im database
        boolean fileAlreadyProcessed = checkIfExistsInDb();
        if (fileAlreadyProcessed) {
            logger.log(Level.FINEST, "File already processed, skipping"); 
            return false;
        }
            
        // transfer md5 file to staging
        boolean fileXferred = getMd5File().transferToVfcuStaging(xferType, true);
        if (!fileXferred) {
            logger.log(Level.FINEST, "Error, unable to transfer md5 file to staging"); 
            return false;
        }
            
        // Insert into Database
        boolean recordInserted = insertDbRecord();
        
        //if md5 inserted successfully, add the files too
        if (!recordInserted) {
            logger.log(Level.FINEST, "Error, unable to insert md5 record into database"); 
            return false;
        }
            
        boolean contentMapPopulated = getMd5File().populateContentsHashMap();
        if (!contentMapPopulated) {
                logger.log(Level.FINEST, "Error, unable to pull contents into HashMap"); 
                return false;
        }
            
        for (String md5Value : getMd5File().getContentsMap().keySet()) {
            MediaFileRecord mediaFileRecord = new MediaFileRecord();
            mediaFileRecord.getVfcuMediaFile().setVfcuMd5FileId(getVfcuMd5File().getVfcuMd5FileId());
            mediaFileRecord.getVfcuMediaFile().setVendorCheckSum(md5Value);
            mediaFileRecord.getVfcuMediaFile().setMediaFileName(getMd5File().getContentsMap().get(md5Value) );
            boolean dbRecordInserted = mediaFileRecord.getVfcuMediaFile().insertRow();
            if (!dbRecordInserted) {
                logger.log(Level.FINEST, "Error, unable to insert mediaFile row"); 
            }    
        }
        return true;
    }
    
    public boolean populateBasicValuesFromDb() {
        vfcuMd5File.populateBasicDbData();
        return true;
    }
    

    public boolean insertDbRecord() {

        boolean recordInserted = vfcuMd5File.insertRecord();
        return recordInserted;
    }

}
