/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.operations.Operation;
import java.util.ArrayList;
import edu.si.damsTools.vfcu.delivery.SourceFileListing;
import edu.si.damsTools.vfcu.delivery.MediaFileRecord;
import edu.si.damsTools.vfcu.database.VfcuMd5ActivityLog;
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
        
  
        
        /*get a listing of all master md5 files and associated subfiles
        //where all the associated  are completed (MD5 validation complete) from batches that are not yet marked as complete */  
        populateMasterMd5ListNotComplete();
        
        
        //Examine each md5 file
        for (SourceFileListing sourceFileListing : masterMd5FileList ) {
            //Examine each listing within each md5 file for completeness
            VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
            ArrayList<Integer> mediaIds = new ArrayList<>();
            vfcuMediaFile.setVfcuMd5FileId(sourceFileListing.getVfcuMd5File().getVfcuMd5FileId());
            
            //We go through all the incomplete batches, and add additional error codes on some records as need be...
            //  whether the batch is completed or not
            mediaIds = vfcuMediaFile.retrieveNoErrorIdsForMd5Id();
            for (Integer mediaId : mediaIds) {
                MediaFileRecord mediaFileRecord = new MediaFileRecord(mediaId);
                mediaFileRecord.populateBasicValuesFromDb();
                mediaFileRecord.validateForCompletion("master");

                 if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
                    //get the associated subfile md5, and do the same validation
                    Integer subFileId = vfcuMediaFile.retrieveSubFileId();
                    MediaFileRecord submediaFileRecord = new MediaFileRecord(subFileId);
                    submediaFileRecord.populateBasicValuesFromDb();
                    submediaFileRecord.validateForCompletion("master");
                 }                
            }
        
            //Check to see if the batch is complete
            int NumFilesInBatch = sourceFileListing.retrieveCountInBatch();
            int NumFilesProcessed = vfcuMediaFile.returnCountFilesForMd5Id();

            if (NumFilesInBatch == NumFilesProcessed) {
                //Look for any files 'left behind' on the file server
                if (DamsTools.getProperty("vldtAllMediaXfered").equals("true")) {
                 
                    int filesInDir = returnFilesInDir();
                    if (filesInDir != NumFilesProcessed) {
                        //STILL HAVE TO DO THIS
                    }
                }

                //Mark batch as completed
                VfcuMd5ActivityLog vfcuMd5ActivityLog = new VfcuMd5ActivityLog();
                vfcuMd5ActivityLog.setVfcuMd5FileId(sourceFileListing.getVfcuMd5File().getVfcuMd5FileId());
                vfcuMd5ActivityLog.setVfcuStatusCd("bC");
                vfcuMd5ActivityLog.insertRecord();
            }
        }            
    }
    
    private int returnFilesInDir() {
        return 0;
    }

    
    private boolean populateMasterMd5ListNotComplete() {
        
        String sql = "SELECT masterfile_vfcu_md5_file_id " +
                     "FROM vfcu_md5_file vmd " + 
                     "INNER JOIN vfcu_md5_file_hierarchy vmfh " +
                     "ON vmfh.masterfile_vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                     "WHERE vmd.project_cd = '" + DamsTools.getProjectCd() + "' " +
                     "AND NOT EXISTS ( " +
                        "SELECT 'X' from vfcu_md5_file_activity_log vmal " +
                        "WHERE vmal.vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                        "AND vmal.vfcu_status_cd = 'BC') ";
        
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
        
        return reqProps;
    }
    
}
