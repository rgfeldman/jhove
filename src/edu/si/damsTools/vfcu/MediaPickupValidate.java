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
import edu.si.damsTools.vfcu.delivery.MediaFileRecord;
import edu.si.damsTools.vfcu.files.xferType.XferTypeFactory;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import edu.si.damsTools.vfcu.utilities.ErrorLog;
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
    private final XferTypeFactory xferTypeFactory;
    private final XferType xferType;
    private final ArrayList<String> reqProps;
    
    public MediaPickupValidate() {
        reqProps = new ArrayList<>();
         
        masterListForBatch = new ArrayList<>();
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
                   errorLog.captureMediaFileError(masterMediaFileRecord.getVfcuMediaFile(), "VMS", null, "Master has no subfile");
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
                    
                //mark as PS status (found matching subfile master and processed both successfully)    
                
                VfcuActivityLog activityLog = new VfcuActivityLog();
                activityLog.setVfcuMediaFileId(masterMediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
                activityLog.setVfcuStatusCd("PS");
                activityLog.insertRow();
                
                activityLog = new VfcuActivityLog();
                activityLog.setVfcuMediaFileId(subFileMediaRecord.getVfcuMediaFile().getVfcuMediaFileId());
                activityLog.setVfcuStatusCd("PS");
                activityLog.insertRow();
            }
                      
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }          
        }
           
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                  
    }       
     
    public ArrayList<String> returnRequiredProps () {
        
        reqProps.add("maxMasterFilesBatch");
        reqProps.add("fileXferType");
        reqProps.add("dupFileNmCheck");
        reqProps.add("useMasterSubPairs");
        //add more required props here
        return reqProps;    
    }
    
    public boolean requireSqlCriteria () {
        return false;
    }
}
