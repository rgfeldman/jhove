/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database.dbRecords;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import java.util.logging.Logger;
import java.util.logging.Level;
/**
 *
 * @author rfeldman
 */
public class BatchFileRecord {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final VfcuMd5File vfcuMd5File;
    
    public BatchFileRecord () {
        vfcuMd5File = new VfcuMd5File();
    }
    
    public BatchFileRecord (Integer vfcuMd5FileId) {
        vfcuMd5File = new VfcuMd5File();
        vfcuMd5File.setVfcuMd5FileId(vfcuMd5FileId);
    }
    
    public VfcuMd5File getVfcuMd5File() {
        return this.vfcuMd5File;
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
    
    
    public boolean populateBasicValuesFromDeliveryFile (String fileName, String filePathEnding) {

        vfcuMd5File.setVendorMd5FileName(fileName);
        vfcuMd5File.setFilePathEnding(filePathEnding);
            
        vfcuMd5File.setBasePathStaging(DamsTools.getProperty("vfcuStaging"));  
        vfcuMd5File.setBasePathVendor(DamsTools.getProperty("vendorBaseDir"));
            
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
    
    public boolean populateBasicValuesFromDb() {
        vfcuMd5File.populateBasicDbData();
        return true;
    }
    

    public boolean insertDbRecord() {

        boolean recordInserted = vfcuMd5File.insertRecord();
        return recordInserted;
    }
    
    boolean excludeMediaFiles(String fileName) {
        //Skip the line if he filename is Thumbs.Db.  It is a temp windows generated file Do not even add it to Database
        if (fileName.equals("Thumbs.db")) {
            return true;
        }
        if (fileName.equals(".DS_Store")) {
            return true;
        }
        
        //skip any filenames that end with .md5 extension
        return fileName.endsWith(".md5");
    }

}
