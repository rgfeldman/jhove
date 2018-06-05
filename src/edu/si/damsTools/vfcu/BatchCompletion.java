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
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
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
    
    private void examineMediaFile (MediaFileRecord mediaFileRecord) {
        

        
    }
    
    
    
    public void invoke () {
        
        /*get a listing of all master md5 files and associated subfiles
        //where all the associated  are completed (MD5 validation complete) from batches that are not yet marked as complete */  
        populateMasterMd5ListToComplete();
        
        //Examine each md5 file
        for (SourceFileListing sourceFileListing : masterMd5FileList ) {
            //Examine each listing within each md5 file for completeness
            VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
            ArrayList<Integer> mediaIds = new ArrayList<>();
            vfcuMediaFile.setVfcuMd5FileId(sourceFileListing.getVfcuMd5File().getVfcuMd5FileId());
            mediaIds = vfcuMediaFile.retrieveNoErrorIdsForMd5Id();
            for (Integer mediaId : mediaIds) {
                MediaFileRecord mediaFileRecord = new MediaFileRecord(mediaId);
                mediaFileRecord.populateBasicValuesFromDb();
                mediaFileRecord.validateForCompletion("master");

                examineMediaFile(mediaFileRecord);
                
                //get the associated subfile md5, and do the same validation
                
            }
            
           
            
        }
        
        
        //Look for Any files in the master or subfile that do not have 'PS' status and mark as error.
        
        //Look for any files 'left behind' on the file server
        
        //Mark batch as completed
        
    }
    
    private boolean populateMasterMd5ListToComplete() {
        
        String statusToCheck = "PM";
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
             statusToCheck = "PS";
        }
        
        String sql = "SELECT masterfile_vfcu_md5_file_id " +
                     "FROM vfcu_md5_file vmd" + 
                     "INNER JOIN vfcu_md5_file_hierarchy vmfh " +
                     "ON vmfh.masterfile_vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                     "WHERE vmd.project_cd = '" + DamsTools.getProjectCd() + "' " +
                     "AND NOT EXISTS ( " +
                        "SELECT 'X' from vfcu_md5_file_activity_log vmal " +
                        "WHERE vmal.vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                        "AND vmal.vfcu_status_cd = 'BC') " +
                    "AND vmd.vfcu_md5_file_id NOT IN ( " +
                        "SELECT DISTINCT md52.vfcu_md5_File_id" +
                        "FROM    vfcu_md5_file md52, " +
                        "vfcu_media_file vmf2 " +
                        "WHERE   vmf2.vfcu_md5_file_id = md52.vfcu_md5_file_id " +
                        "AND     md52.project_cd = '" + DamsTools.getProjectCd() + "' "  +
                        "AND NOT EXISTS ( " +
                            "SELECT 'X' " +
                            "FROM  vfcu_activity_log val " +
                            "WHERE val.vfcu_media_file_id = vmf2.vfcu_media_file_id " +
                            "AND   val.vfcu_status_cd in ('" + statusToCheck +"' ,'ER')))";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
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
        
        return reqProps;
    }
    
}
