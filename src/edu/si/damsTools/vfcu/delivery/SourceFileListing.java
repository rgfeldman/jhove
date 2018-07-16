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
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.List;
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

        vfcuMd5File.setVendorMd5FileName(md5File.getFileName().toString());
        vfcuMd5File.setFilePathEnding(md5File.getFilePathEnding("source").toString() );
            
        vfcuMd5File.setBasePathStaging(DamsTools.getProperty("vfcuStaging"));  
        vfcuMd5File.setBasePathVendor(DamsTools.getProperty("sourceBaseDir"));
            
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
            
        for (String filename : getMd5File().getContentsMap().keySet()) {
            MediaFileRecord mediaFileRecord = new MediaFileRecord();
            mediaFileRecord.getVfcuMediaFile().setVfcuMd5FileId(getVfcuMd5File().getVfcuMd5FileId());
            mediaFileRecord.getVfcuMediaFile().setMediaFileName(filename);
            mediaFileRecord.getVfcuMediaFile().setVendorCheckSum(getMd5File().getContentsMap().get(filename) );
            boolean dbRecordInserted = mediaFileRecord.getVfcuMediaFile().insertRow();
            if (!dbRecordInserted) {
                logger.log(Level.FINEST, "Error, unable to insert mediaFile row"); 
            }    
        }
        return true;
    }
    
    public boolean populateBasicValuesFromDbVendor() {
        
        vfcuMd5File.populateBasicDbData();
        
        String fileNamePath = vfcuMd5File.getBasePathVendor()+ "/" + vfcuMd5File.getFilePathEnding() + "/" + vfcuMd5File.getVendorMd5FileName();   
        md5File = new Md5File(Paths.get(fileNamePath));
       
        return true;
    }
    

    public boolean insertDbRecord() {

        boolean recordInserted = vfcuMd5File.insertRecord();
        return recordInserted;
    }
    
    public int retrieveCountProcessed() {
        int numProcessed = 0;
        String statusToCheck = "PM";
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
             statusToCheck = "PS";
        }
        
        String sql = "SELECT count(*) " +
                     "FROM vfcu_media_file vmf " +
                     "INNER JOIN vfcu_activity_log val " +
                     "ON vmf.vfcu_media_file_id = val.vfcu_media_file_id " +
                     "WHERE val.vfcu_status_cd in ('ER','" + statusToCheck + "')" +
                     "AND vfcu_md5_file_id = " + this.vfcuMd5File.getVfcuMd5FileId() ;
                     
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                numProcessed = rs.getInt(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to get count of completed records", e );
        }

        return numProcessed;
    }
    
    public int retrieveCountInVendorFileSystem() {
        String vendorFileSystemDir = null;
        
        if (vfcuMd5File.getFilePathEnding() != null) {
            vendorFileSystemDir = this.vfcuMd5File.getBasePathVendor() + "/" + vfcuMd5File.getFilePathEnding();
        }
        else {
            vendorFileSystemDir = this.vfcuMd5File.getBasePathVendor();
        }
        
        List<String> fileNames = new ArrayList<>();
        
        try {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(vendorFileSystemDir));
            for (Path path : directoryStream) {
                fileNames.add(path.toString());
            }
        } catch (Exception e) {
           logger.log(Level.FINER, "Error: unable to get count of files", e );
        }
    
        logger.log(Level.FINER, "Number of files in vendor filesystem: " + fileNames.size());
        
        return fileNames.size();
    }

}
