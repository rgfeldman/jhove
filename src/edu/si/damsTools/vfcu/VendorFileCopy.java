/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.cdis.Operation;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.vfcu.database.dbRecords.BatchFileRecord;
import edu.si.damsTools.vfcu.database.dbRecords.MediaFileRecord;
import edu.si.damsTools.vfcu.deliveryfiles.MediaFile;
import edu.si.damsTools.vfcu.files.xferType.XferTypeFactory;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import edu.si.damsTools.vfcu.utilities.ErrorLog;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VendorFileCopy extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());    
    
    private final ArrayList<MediaFileRecord> fileListForBatch;
    private final ArrayList<Integer> distinctMd5FileIds;
    
    public VendorFileCopy() {
        fileListForBatch = new ArrayList<>();
        distinctMd5FileIds = new ArrayList<>();
    }
    
    private boolean assignToCurrentBatch() {
        
        //Assign database entry rows to a batch
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setMaxFiles(Integer.parseInt(DamsTools.getProperty("maxFilesBatch")));
        vfcuMediaFile.setVfcuBatchNumber(DamsTools.getBatchNumber());
        
        int rowsUpdated = vfcuMediaFile.updateVfcuBatchNumber();
        return rowsUpdated >= 1;
    }
    
    private boolean populateMediaFilesForBatch() {
    
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuBatchNumber(DamsTools.getBatchNumber());
        ArrayList<Integer> fileIdsForBatch = new ArrayList<>();  
        fileIdsForBatch = vfcuMediaFile.returnFileIdsForBatch();
        
        for (Integer fileId: fileIdsForBatch) {
            MediaFileRecord mediaFileRecord = new MediaFileRecord(fileId);
            mediaFileRecord.populateBasicValuesFromDb();
            fileListForBatch.add(mediaFileRecord);
        }
        return true;
    }
    
    public void invoke () {
    
        //setBatchNumber for current batch
        boolean filesAssignedToBatch = assignToCurrentBatch();
        if (! filesAssignedToBatch) {
            //no files found that can be assigned to a validate and copy batch.  We have no need to go further
            logger.log(Level.FINER, "No files found to process");
            return;
        }
        //Now we updated the files and assigned to current batch, commit so we lock them into current batch
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
      
        XferTypeFactory xferTypeFactory = new XferTypeFactory();
        XferType xferType = xferTypeFactory.XferTypeChooser();
        
        //create array of MediaFiles just assigned to batch     
        populateMediaFilesForBatch();
        
        for (MediaFileRecord mediaFileRecord: fileListForBatch) {
            
            BatchFileRecord batchFileRecord = new BatchFileRecord(mediaFileRecord.getVfcuMediaFile().getVfcuMd5FileId());
            batchFileRecord.populateBasicValuesFromDb();
            String fileLoc = batchFileRecord.returnStringBatchDir() + "/" + mediaFileRecord.getVfcuMediaFile().getMediaFileName();
             logger.log(Level.FINER, "fileLoc:" + fileLoc);
            Path pathFile = Paths.get(fileLoc);
            
            MediaFile mediaFile = new MediaFile(pathFile);
            
            boolean mediaTransfered = mediaFile.transferToDAMSStaging(xferType, false);   
            
            if (!mediaTransfered) {
                ErrorLog errorLog = new ErrorLog(); 
                errorLog.capture(mediaFileRecord.getVfcuMediaFile(), xferType.returnXferErrorCode(), "Failure to xfer Vendor File");        
                continue;
            }
                
            //Insert the code indicating that the file was just transferred
            VfcuActivityLog activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
            activityLog.setVfcuStatusCd(xferType.returnCompleteXferCode());
            activityLog.insertRow();
                
                
            //Populate attributes post-file transfer
            mediaFile.populateAttributes();
            mediaFileRecord.getVfcuMediaFile().setVfcuChecksum(mediaFile.getMd5Hash());
            mediaFileRecord.getVfcuMediaFile().setMediaFileSize(mediaFile.getMediaFileSize());
            mediaFileRecord.getVfcuMediaFile().setMediaFileDate(mediaFile.getMediaFileDate());
            mediaFileRecord.getVfcuMediaFile().updateVfcuMediaAttributes();           
            
            //Perform validations on the physical files
            String errorCode = mediaFile.validate();
            if (errorCode != null) {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(mediaFileRecord.getVfcuMediaFile(), errorCode, "Validation Failure");
                return;
            }
            
            //If we validated with jhove, now we need to record this in the datbase
            if (mediaFile.retJhoveValidated()) {
                activityLog.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
                activityLog.setVfcuStatusCd("JH");
                activityLog.insertRow();
            }
  
            if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
                mediaFileRecord.genAssociations(batchFileRecord.getVfcuMd5File().getFileHierarchyCd());
            }
            
            //Perform validations on the database Record
            mediaFileRecord.validate();
            
            if (! distinctMd5FileIds.contains(mediaFileRecord.getVfcuMediaFile().getVfcuMd5FileId())) {
                distinctMd5FileIds.add(mediaFileRecord.getVfcuMediaFile().getVfcuMd5FileId());
            }
        }
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            masterSubfileValidation();
        }
           
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                  
    }       
    
    private boolean masterSubfileValidation() {
        
        for (Integer vfcuMd5FileId : distinctMd5FileIds) {
           
            ArrayList<Integer> parentFileErrorList = new  ArrayList<>();
            
            //check status of files in the md5 batch to see if any remain incomplete
            parentFileErrorList = returnParChildErrorList(vfcuMd5FileId);
            
            for (Integer errorMediaFileId : parentFileErrorList) {
                
                BatchFileRecord batchFileRecord = new BatchFileRecord();
                MediaFileRecord mediaFileRecord  = new MediaFileRecord();
                ErrorLog errorLog = new ErrorLog();
                
                batchFileRecord.getVfcuMd5File().setVfcuMd5FileId(vfcuMd5FileId);
                mediaFileRecord.getVfcuMediaFile().setVfcuMediaFileId(errorMediaFileId);
                
                if(batchFileRecord.getVfcuMd5File().getFileHierarchyCd().equals("M")) {
                    errorLog.capture(mediaFileRecord.getVfcuMediaFile(), "VMS", "Master has no subfile");
                }
                else if(batchFileRecord.getVfcuMd5File().getFileHierarchyCd().equals("S")) {  
                    errorLog.capture(mediaFileRecord.getVfcuMediaFile(), "VSM", "Subfile has no master");
                } 
            }
        }
        
        return true;
        
    }
    
     public ArrayList<Integer> returnParChildErrorList (Integer vfcuMd5FileId) {
        ArrayList<Integer> parentFileErrorList = new  ArrayList<>();
        
        //get the count where some records have been primary/secondary validated, but not all have
        
        String sql = "SELECT 'X' " +
                     "FROM vfcu_media_file vmf" +
                     "WHERE vfcu_md5_file = " + vfcuMd5FileId +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' from vfcu_activity_log vfa " +
                        "WHERE vfa.vfcu_media_file = vfa.vfcu_media_File " +
                        "AND vfa.activity_log in ('PS') ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 // no need to validate, there have been no parent/child validations, we could be waiting on the other file
                 return null;
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for status of MD5", e );
            return null;
        }
        
        sql = "SELECT vmf.vfcu_media_file " +
                     "FROM vfcu_media_file vmf" +
                     "WHERE vfcu_md5_file = " + vfcuMd5FileId +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' from vfcu_activity_log vfa " +
                        "WHERE vfa.vfcu_media_file = vfa.vfcu_media_File " +
                        "AND vfa.activity_log in ('ER','PS') ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            while (rs.next()) {
                 parentFileErrorList.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Count completed records for MD5", e );
        }
        
        return parentFileErrorList;
    }
     
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("maxFilesBatch");
        reqProps.add("fileXferType");
        reqProps.add("dupFileNmCheck");
        reqProps.add("useMasterSubPairs");
        //add more required props here
        return reqProps;    
    }
}
