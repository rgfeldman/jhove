/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.vfcu.database.VFCUActivityLog;
import edu.si.damsTools.vfcu.database.VFCUMediaFile;
import edu.si.damsTools.vfcu.database.VFCUMd5File;
import edu.si.damsTools.vfcu.files.MediaFile;
import edu.si.damsTools.vfcu.files.VendorMd5File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class VendorFileCopy {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());    
    
    
    private void finalValidations() {
        
        HashMap <Integer, String> idsCurrentBatch;
        idsCurrentBatch = new HashMap<> (); 
        
        // GET unique md5 file ids to validate.
        idsCurrentBatch = returnIdsForFinalVldtn();
        
        //go through the list array list at a time
        for (Integer currentMd5FileId : idsCurrentBatch.keySet()) {  
            VendorMd5File vendorMd5File = new VendorMd5File();
            
            vendorMd5File.setMd5FileId(currentMd5FileId);  
            String masterIndStr = idsCurrentBatch.get(currentMd5FileId);
            
            if ((masterIndStr).equals("master")) {
                vendorMd5File.setMasterIndicator(true);
            }
            else {        
                vendorMd5File.setMasterIndicator(false);
            }
             
            boolean md5ContentsVerified = vendorMd5File.finalValidations();
            
            if (! md5ContentsVerified) {
                logger.log(Level.FINEST, "MD5 contents not verified, pending items still" );
                continue;
            }
          
            //We add the PS status only if there is a master/subfile relationship, which is why we use the subfile
            //to drive this process.
            
            if (masterIndStr.equals("subfile")) {
                VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
                vfcuMediaFile.setVfcuMd5FileId(currentMd5FileId);
                HashMap<Integer, String> fileNameId;
                fileNameId = new HashMap<> ();
                 
                fileNameId = vfcuMediaFile.returnSuccessFileNmsIdForMd5Id ();
                
                // Loop through each subfile in list
                // For each subfile in list, see if there is a corresponding master file
                for (Integer subFileMediaId : fileNameId.keySet()) { 
                    vfcuMediaFile = new VFCUMediaFile();
                    vfcuMediaFile.setVfcuMediaFileId(subFileMediaId);
                    vfcuMediaFile.setMediaFileName(fileNameId.get(subFileMediaId));
                    
                    VFCUMd5File vfcuMd5File = new VFCUMd5File();
                    
                    //find the masterMd5FileID for the current md5 FileId
                    vfcuMd5File.setVfcuMd5FileId(currentMd5FileId);
                    vfcuMd5File.populateMasterMd5FileId();
                    
                    //Set the md5 FileId to the masterMd5FileID that we just obtained
                    vfcuMediaFile.setVfcuMd5FileId(vfcuMd5File.getMasterMd5FileId());
                    Integer masterFileId = vfcuMediaFile.returnIdForMd5IdBaseName();
                    
                    //Add the PS status to indicate primary/secondary relationship has been established
                    if (masterFileId != null ) {
                        //We have a master and subfile ID
                        VFCUActivityLog activityLog = new VFCUActivityLog();
                        activityLog.setVfcuMediaFileId(subFileMediaId);
                        activityLog.setVfcuStatusCd("PS");
                        //insert a verification complete row for the subfile
                        activityLog.insertRow();
                        
                        activityLog = new VFCUActivityLog();
                        activityLog.setVfcuMediaFileId(masterFileId);
                        activityLog.setVfcuStatusCd("PS");
                        //insert a verification complete row for the Master
                        activityLog.insertRow();
                    }
                }
            }        
        }    
    }
    
    
    private HashMap returnIdsForFinalVldtn () {

        //Returns list of md5 file ids in current batch, 
        //--Exclude from the list if it contains files not yet assigned to a VFCU 
        //--Exclude from the list if it contains files that already have PS status
        //--Exclude from the list if the VFCU report has already been generated
        
        HashMap <Integer, String> idsCurrentBatch;
        idsCurrentBatch = new HashMap<> (); 
        
        String sql =    "SELECT DISTINCT b.vfcu_md5_file_id, a.master_md5_file_id " +
                        "FROM   vfcu_md5_file a, " +
                        "       vfcu_media_file b " +
                        "WHERE  a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                        "AND    a.project_cd = '" + DamsTools.getProjectCd() + "' " +
                        "AND    b.vfcu_batch_number = '" + DamsTools.getBatchNumber() + "' " +
                        "AND    a.vfcu_rpt_dt IS NULL " +
                        "AND NOT EXISTS ( " + 
                            "SELECT 'X' FROM vfcu_media_file c " + 
                            "WHERE  a.vfcu_md5_file_id = c.vfcu_md5_file_id " + 
                            "AND    c.vfcu_batch_number is null) " +
                        "AND NOT EXISTS ( " +
                            "SELECT 'X' FROM vfcu_activity_log d " +
                            "WHERE d.vfcu_media_file_id = b.vfcu_media_file_id " +
                            "AND d.vfcu_status_cd = 'PS') ";
                     
        logger.log(Level.FINEST,"SQL! " + sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs.next()) {
                if (rs.getInt(1) == rs.getInt(2)) {
                    idsCurrentBatch.put(rs.getInt(1), "master");
                }
                else {
                    idsCurrentBatch.put(rs.getInt(1), "subfile");
                }
            }
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
        }
        return idsCurrentBatch;
        
    }
    
    public void validateAndCopy() {
    
        //Assign database entry rows to a batch
        VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
        vfcuMediaFile.setMaxFiles(Integer.parseInt(DamsTools.getProperty("maxFilesBatch")));
        vfcuMediaFile.setVfcuBatchNumber(DamsTools.getBatchNumber());
        
        int rowsUpdated = vfcuMediaFile.updateVfcuBatchNumber();
        if (rowsUpdated < 1 ) {
            logger.log(Level.FINEST, "No files found in DB that require copy and validation" );
        }
            
        //Now we updated the files and assigned to current batch, commit so we lock them into current batch
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
      
        //create array of files just assigned to batch
        vfcuMediaFile.populateFilesIdsForBatch();
        
        for (Iterator<Integer> iter = vfcuMediaFile.getFilesIdsForBatch().iterator()  ; iter.hasNext();) {
        //no files found that can be assigned to a validate and copy batch.  We have no need to go further
                
            MediaFile mediaFile = new MediaFile();
            mediaFile.setVfcuMediaFileId(iter.next() );
            boolean mediaTransfered = mediaFile.transfer();
            
            if (mediaTransfered) {
                //Perform MD5 file validations once the file has been xferred
                mediaFile.validate();
            }
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
              
        }       
        //do final validations and status entries that need to be done AFTER the whole batch was xferred
        finalValidations();
    }             
    
}
