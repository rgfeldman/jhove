/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.vfcu.delivery.SourceFileListing;
import edu.si.damsTools.vfcu.delivery.MediaFileRecord;
import edu.si.damsTools.vfcu.delivery.files.MediaFile;
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
public class MediaPickupValidate extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());    
    
    private final ArrayList<MediaFileRecord> masterListForBatch;
    private final ArrayList<Integer> distinctMd5FileIds;
    private final XferTypeFactory xferTypeFactory;
    private final XferType xferType;
    
    public MediaPickupValidate() {
        masterListForBatch = new ArrayList<>();
        distinctMd5FileIds = new ArrayList<>();
        xferTypeFactory = new XferTypeFactory();
        xferType = xferTypeFactory.XferTypeChooser();
        
    }
    
    /*Method: assignToPickupValidateBatch 
      Description: Assigns a set of records in the Database to the current batch.
                    We keep it simple and only update the database, we want the code to go through this 
                    part of the code as quickly as possible to lock these files from other processes
    */
    private boolean assignToPickupValidateBatch() {
        
        //Assign database entry rows to a batch
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setMaxFiles(Integer.parseInt(DamsTools.getProperty("maxMasterFilesBatch")));
        vfcuMediaFile.setVfcuBatchNumber(DamsTools.getBatchNumber());
        
        int rowsUpdated = vfcuMediaFile.updatePickupValidateBatch();
        return rowsUpdated >= 1;
    }
    
    /*Method: populateMediaFilesForBatch 
      Description: Creates the Array holding the mediaFile information for each of the files in the current batch
    */
    private boolean populateMediaFilesForBatch() {
    
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuBatchNumber(DamsTools.getBatchNumber());
        ArrayList<Integer> fileIdsForBatch = new ArrayList<>();  
        fileIdsForBatch = vfcuMediaFile.returnVfcuFileIdsForBatch();
        
        for (Integer fileId: fileIdsForBatch) {
            MediaFileRecord mediaFileRecord = new MediaFileRecord(fileId);
            mediaFileRecord.populateBasicValuesFromDb();
            masterListForBatch.add(mediaFileRecord);
        }
        return true;
    }
    
    public void invoke () {
    
        //lock a set of records into this batch in the database.
        boolean filesAssignedToBatch = assignToPickupValidateBatch();
        if (! filesAssignedToBatch) {
            //no files found that can be assigned to a validate and copy batch.  We have no need to go further
            logger.log(Level.FINER, "No files found to process");
            return;
        }
        //commit is what lock them into current batch so other processes dont grab them
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }

        //create array of MediaFiles just locked into this batch     
        populateMediaFilesForBatch();
        
        for (MediaFileRecord masterMediaFileRecord: masterListForBatch) {
            
            boolean validXferred = masterMediaFileRecord.validateAndTransfer(xferType);
            if (! validXferred) {
                //error encountered and logged
                logger.log(Level.FINER, "logged error that was encountered with file");
                continue;
            }
            
            // Now must do the same for the subfile...and sub-subfile
            if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
               Integer assocVfcuMediaFileId = masterMediaFileRecord.getVfcuMediaFile().returnAssociatedFileId();
               if (assocVfcuMediaFileId == null) {
                   logger.log(Level.FINER, "Error, master found with no subfile");
                   ErrorLog errorLog = new ErrorLog();
                   errorLog.capture(masterMediaFileRecord.getVfcuMediaFile(), "VMS", "Master has no subfile");
                   continue;
                }
                masterMediaFileRecord.getVfcuMediaFile().setChildVfcuMediaFileId(assocVfcuMediaFileId);
                masterMediaFileRecord.getVfcuMediaFile().updateChildVfcuMediaFileId();
               
                MediaFileRecord subFileMediaRecord = new MediaFileRecord(assocVfcuMediaFileId); 
                subFileMediaRecord.populateBasicValuesFromDb();
                subFileMediaRecord.getVfcuMediaFile().updateVfcuBatchNumber();
                
                validXferred = subFileMediaRecord.validateAndTransfer(xferType);
                    if (! validXferred) {
                    //error encountered and logged
                    logger.log(Level.FINER, "logged error that was encountered with file");
                    continue;
                }
               
            }
                
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
             
        }
        
       //if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
       //     masterSubfileValidation();
        //}
           
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                  
    }       
    
    
    
    private boolean masterSubfileValidation() {
        
        
        // ALREADY DID VMS, need to look for VSM
        
        
        for (Integer vfcuMd5FileId : distinctMd5FileIds) {
           
            ArrayList<Integer> parentFileErrorList = new  ArrayList<>();
            
            //check status of files in the md5 batch to see if any remain incomplete
            parentFileErrorList = returnParChildErrorList(vfcuMd5FileId);
            
            if (parentFileErrorList == null || parentFileErrorList.isEmpty()) {
                //nothing to validate yet
                return true;
            }
            
            for (Integer errorMediaFileId : parentFileErrorList) {
                
                SourceFileListing batchFileRecord = new SourceFileListing();
                MediaFileRecord mediaFileRecord  = new MediaFileRecord();
                ErrorLog errorLog = new ErrorLog();
                
                batchFileRecord.getVfcuMd5File().setVfcuMd5FileId(vfcuMd5FileId);
                mediaFileRecord.getVfcuMediaFile().setVfcuMediaFileId(errorMediaFileId);
                
               // if(batchFileRecord.getVfcuMd5File().getFileHierarchyCd().equals("M")) {
                //    errorLog.capture(mediaFileRecord.getVfcuMediaFile(), "VMS", "Master has no subfile");
               // }
               // else if(batchFileRecord.getVfcuMd5File().getFileHierarchyCd().equals("S")) {  
                //    errorLog.capture(mediaFileRecord.getVfcuMediaFile(), "VSM", "Subfile has no master");
                //} 
            }
        }
        
        return true;
        
    }
    
     public ArrayList<Integer> returnParChildErrorList (Integer vfcuMd5FileId) {
        ArrayList<Integer> parentFileErrorList = new  ArrayList<>();
        
        //get the count where some records have been primary/secondary validated, but not all have
        
        //First see if there are any records missing 'PM' status.  If some are missing 'PM' status we cannot expect PS to be ready yet for all of them
        String sql = "SELECT 'X' " +
                     "FROM vfcu_media_file vmf " +
                     "WHERE vfcu_md5_file_id = " + vfcuMd5FileId +
                     " AND NOT EXISTS (" +
                        "SELECT 'X' from vfcu_activity_log vfa " +
                        "WHERE vfa.vfcu_media_file_id = vmf.vfcu_media_File_id " +
                        "AND vfa.vfcu_status_cd in ('PM','ER')) ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 // no need to validate, there have been no parent/child validations, we could be waiting on the other file
                 logger.log(Level.FINER, "Batch Not complete, all records do not have PM yet");
                 return null;
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for status of MD5", e );
            return null;
        }
        
        //Now see if there are ANY records with 'PS' status.  If there are no records with 'PS' yet, then the associated file has not yet been put in VFCU
        // note, this is not the best way...and may need some work to detect condition where an assoc md5 has not been processed yet
        sql = "SELECT 'X' " +
              "FROM vfcu_media_file vmf " +
              "WHERE vfcu_md5_file_id = " + vfcuMd5FileId +
              " AND EXISTS (" +
                "SELECT 'X' from vfcu_activity_log vfa " +
                "WHERE vfa.vfcu_media_file_id = vmf.vfcu_media_File_id " +
                "AND vfa.vfcu_status_cd in ('PS','ER')) ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (! rs.next()) {
                // no need to validate, none of the records have PS yet
                logger.log(Level.FINER, "Batch Not complete, still awaiting associated records");
                return null;
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for status of MD5", e );
            return null;
        }
        
        sql = "SELECT vmf.vfcu_media_file_id " +
                     "FROM vfcu_media_file vmf " +
                     "WHERE vfcu_md5_file_id = " + vfcuMd5FileId +
                     " AND NOT EXISTS (" +
                        "SELECT 'X' from vfcu_activity_log vfa " +
                        "WHERE vfa.vfcu_media_file_id = vmf.vfcu_media_File_id " +
                        "AND vfa.vfcu_status_cd in ('ER','PS')) ";
        
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
        
        reqProps.add("maxMasterFilesBatch");
        reqProps.add("fileXferType");
        reqProps.add("dupFileNmCheck");
        reqProps.add("useMasterSubPairs");
        //add more required props here
        return reqProps;    
    }
}
