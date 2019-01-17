/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.utilities.XmlUtils;
import java.util.ArrayList;
import edu.si.damsTools.vfcu.delivery.SourceFileListing;
import edu.si.damsTools.vfcu.delivery.MediaFileRecord;
import edu.si.damsTools.vfcu.database.VfcuMd5FileActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMd5FileHierarchy;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class BatchCompletion extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<SourceFileListing> masterMd5FileList;
    private final ArrayList<String> reqProps;
    
    public BatchCompletion() {
        masterMd5FileList = new ArrayList<>();
        reqProps = new ArrayList<>();
    }
    
    public void determineIfComplete() {
        
    }
    
    public void invoke () {
        
        //get a listing of all master md5 files that have yet to be marked complete in the database
        populateMasterMd5ListNotComplete();
        
        //Examine each master md5 file
        for (SourceFileListing sourceFileListing : masterMd5FileList ) {
            
            //perform commit at start of loop, so it is commited for each md5 file
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
            VfcuMediaFile masterVfcuMediaFile = new VfcuMediaFile();
            VfcuMediaFile subfileVfcuMediaFile = new VfcuMediaFile();
            
            //Examine each listing within each md5 file for completeness
            ArrayList<Integer> masterMediaIds = new ArrayList<>();
            masterVfcuMediaFile.setVfcuMd5FileId(sourceFileListing.getVfcuMd5File().getVfcuMd5FileId());
            
            int totalFilesInDbBatch =  masterVfcuMediaFile.returnCountFilesForMd5Id();
            int numFilesProcessed = masterVfcuMediaFile.getNumCompleteFilesForMd5FileId();
            
            //We go through all the incomplete batches, and add additional error codes on some records as need be...
            //  whether the batch is completed or not
            masterMediaIds = masterVfcuMediaFile.retrieveNoErrorIdsForMd5Id();
            for (Integer mediaId : masterMediaIds) {
                //Check to see if 'PM' status has been done yet, if not skip this record.
                MediaFileRecord masterMediaFileRecord = new MediaFileRecord(mediaId);
                masterMediaFileRecord.checkAssociatedFileForFailure("master");         
            }

            //look to see if we should expect associated subfiles for the masters 
            if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true")) {
                
                //Check to see if master is complete, we only validate subfile if master is complete (or errored)
                if (totalFilesInDbBatch != numFilesProcessed ) {
                    logger.log(Level.FINEST,"Still processing Md5 file: " + numFilesProcessed + " / " + totalFilesInDbBatch);         
                    continue;
                }
                
                //get the subfild id from the database
                VfcuMd5FileHierarchy vfcuMd5FileHierarchy = new VfcuMd5FileHierarchy();
                vfcuMd5FileHierarchy.setMasterFileVfcuMd5FileId(masterVfcuMediaFile.getVfcuMd5FileId());
                vfcuMd5FileHierarchy.populateSubfileIdForMasterId();
                
                subfileVfcuMediaFile.setVfcuMd5FileId(vfcuMd5FileHierarchy.getSubFileVfcuMd5FileId());
                
                //We go through all the incomplete batches, and add additional error codes on some records as need be...
                //  whether the batch is completed or not
                ArrayList<Integer> subMediaIds = new ArrayList<>();
                subMediaIds = subfileVfcuMediaFile.retrieveNoErrorIdsForMd5Id();
                for (Integer subMediaId : subMediaIds) {
                    MediaFileRecord masterMediaFileRecord = new MediaFileRecord(subMediaId);
                    masterMediaFileRecord.checkAssociatedFileForFailure("subfile");         
                }
            }
      
            //Check to see if the batch is complete
            //Get the subfile from the database
            if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true")) {

                totalFilesInDbBatch = totalFilesInDbBatch + subfileVfcuMediaFile.returnCountFilesForMd5Id();
                numFilesProcessed = numFilesProcessed + subfileVfcuMediaFile.getNumCompleteFilesForMd5FileId();           
            }

            //All files in the database were processed
            if (totalFilesInDbBatch == numFilesProcessed) {
                
                //Look for any files 'left behind' on the file server
                if (XmlUtils.getConfigValue("vldtAllMediaXfered").equals("true")) {
                    int countInSourceLocation = sourceFileListing.retrieveCountInVendorFileSystem();
                    logger.log(Level.FINEST,"Count in SourceLocation! " + countInSourceLocation);                    
                }

                //Mark Master batch as completed
                VfcuMd5FileActivityLog vfcuMd5ActivityLog = new VfcuMd5FileActivityLog();
                vfcuMd5ActivityLog.setVfcuMd5FileId(sourceFileListing.getVfcuMd5File().getVfcuMd5FileId());
                vfcuMd5ActivityLog.setVfcuMd5StatusCd("BC");
                vfcuMd5ActivityLog.insertRecord();
                
                //Mark child as completed as well
                if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true")) {
  
                    vfcuMd5ActivityLog = new VfcuMd5FileActivityLog();
                    vfcuMd5ActivityLog.setVfcuMd5FileId(subfileVfcuMediaFile.getVfcuMd5FileId());
                    vfcuMd5ActivityLog.setVfcuMd5StatusCd("BC");
                    vfcuMd5ActivityLog.insertRecord();
                }
            }
            else {
                logger.log(Level.FINEST,"Md5 ID Not complete, DB Batch: " + totalFilesInDbBatch); 
                logger.log(Level.FINEST,"Md5 ID Not complete, Processed count: " + numFilesProcessed); 
            }
        }            
    }

    
    private boolean populateMasterMd5ListNotComplete() {
        
        String sql = "SELECT masterfile_vfcu_md5_file_id " +
                     "FROM vfcu_md5_file vmd " + 
                     "INNER JOIN vfcu_md5_file_hierarchy vmfh " +
                     "ON vmfh.masterfile_vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                     "WHERE vmd.project_cd = '" + XmlUtils.getConfigValue("projectCd") + "' " +
                     "AND NOT EXISTS ( " +
                        "SELECT 'X' from vfcu_md5_file_activity_log vmal " +
                        "WHERE vmal.vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                        "AND vmal.vfcu_md5_status_cd = 'BC') ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            while (rs.next()) {
                SourceFileListing sourceFileListing  = new SourceFileListing(rs.getInt(1));
                sourceFileListing.populateBasicValuesFromDbVendor();
                this.masterMd5FileList.add(sourceFileListing);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
        }
   
        return true;
    }
        
    public ArrayList<String> returnRequiredProps () {

        reqProps.add("useMasterSubPairs");
        reqProps.add("vldtAllMediaXfered");
        reqProps.add("fileXferType");
        
        return reqProps;
    }
    
    public boolean requireSqlCriteria () {
        return false;
    }
    
}
