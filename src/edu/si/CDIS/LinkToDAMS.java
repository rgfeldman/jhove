/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

/**
 *
 * @author rfeldman
 */

import edu.si.CDIS.Database.CDISMap;
import java.io.File;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.DAMS.Database.Uois;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.si.CDIS.DAMS.StagedFile;
import edu.si.CDIS.DAMS.Database.SiPreservationMetadata;
import edu.si.CDIS.Database.CDISActivityLog;
        
public class LinkToDAMS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String assetDate;
    String vendorChecksum;
    String pathBase;
    String pathEnding;
    
    public void link (CDIS cdis) {
        
        // now find all the unlinked images in DAMS (uoiid is null)
        CDISMap cdisMap = new CDISMap();
        
        HashMap<Integer, String> unlinkedDamsRecords;
        unlinkedDamsRecords = new HashMap<> ();
        
        unlinkedDamsRecords = cdisMap.returnUnlinkedMediaHash(cdis.damsConn);
                
        // Now look in all hot folder failed folders to check if there are files there, if there are, record them in error log
        String hotFolderBaseName = null;
        File hotFolderBase;
        
        boolean lastHotFolder = false;
        int hotFolderIncrement = 1;
        
        String failedFolderNm = null;
        File failedHotFolder = null;
        
        while (!lastHotFolder) {
        
            hotFolderBaseName = cdis.properties.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
            hotFolderBase = new File (hotFolderBaseName);
         
            if (hotFolderBase.exists()) {
                int numFailedFiles = 0;

                //count files in hotfolder master area
                failedFolderNm = hotFolderBaseName + "\\FAILED";
                failedHotFolder = new File(failedFolderNm);
                
                if (failedHotFolder.exists()) {
                    numFailedFiles = failedHotFolder.list().length;
                }
                else {
                    logger.log(Level.FINER, "ERROR: Could not find MASTER hotfolder directory in: ", failedFolderNm);
                    break;
                }
                
                if (numFailedFiles == 0) {
                    //We have no failures
                    logger.log(Level.FINER, "There are no files in the FAILED directory " );
                }
                else {
                    logger.log(Level.FINER, "ERROR: found FAILED files in: ", failedFolderNm);
                    File[] listOfFiles = failedHotFolder.listFiles();
                    
                     for (int i = 0; i < listOfFiles.length; i++) {
                         logger.log(Level.FINER, "Failed File: " + listOfFiles[i].getName() );
                     }
                }
            }
        }
        
        // See if we can find the media in DAMS based on the filename and checksum combination
        for (Integer key : unlinkedDamsRecords.keySet()) {  

            Uois uois = new Uois();
            cdisMap = new CDISMap();
            CDISActivityLog activityLog = new CDISActivityLog();
      
            cdisMap.setCdisMapId(key);
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            
            // populate the cdis vfcuId and fileName
            boolean namePopulated = cdisMap.populateNameVfcuId(cdis.damsConn);
            if (! namePopulated) {
                logger.log(Level.FINER, "ERROR: unable to get FileName for map_id: " + cdisMap.getCdisMapId());
                continue;
            }
            
            retrieveVfcuData(cdis.damsConn, cdisMap.getVfcuMediaFileId());
            
            uois.setName(unlinkedDamsRecords.get(key));
            boolean uoiidFound = uois.populateUoiidForNameChksum(cdis.damsConn, vendorChecksum);
            
            if (!uoiidFound) {
                logger.log(Level.FINER, "No matches in DAMS for filename/checksum " + uois.getName() );
                continue;
            }
            
            cdisMap.setUoiid(uois.getUoiid());
            
            boolean uoiidUpdated = cdisMap.updateUoiid(cdis.damsConn);
            if (!uoiidUpdated) {
                logger.log(Level.FINER, "ERROR: unable to update UOIID in DAMS for uoiid: " + uois.getUoiid() );
                continue;
            }
            activityLog.setCdisStatusCd("LDC");
            activityLog.insertActivity(cdis.damsConn);
            
            // Move any .tif files from staging to NMNH EMu pick up directory (on same DAMS Isilon cluster)
            
            if (cdisMap.getFileName().endsWith(".tif")) {
                StagedFile stagedFile = new StagedFile();
                stagedFile.setBasePath(pathBase);
                stagedFile.setPathEnding(pathEnding);
                stagedFile.setFileName(cdisMap.getFileName());
            
                stagedFile.moveToEmu(cdis.properties.getProperty("emuPickupLocation"));
                activityLog.setCdisStatusCd("FME");
                activityLog.insertActivity(cdis.damsConn);
            }
            
            // Create an EMu_ready.txt file in the EMu pick up directory.
                
            // Update the DAMS checksum information in preservation module
            SiPreservationMetadata siPreservation = new SiPreservationMetadata();
            siPreservation.setUoiid(cdisMap.getDamsUoiid()); 
            siPreservation.setAssetSourceDate(this.assetDate);
            siPreservation.setPreservationIdNumber(this.vendorChecksum);
            siPreservation.insertRow(cdis.damsConn);
            
            activityLog.setCdisStatusCd("VCD");
            activityLog.insertActivity(cdis.damsConn);
            
            try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
        }
        
    }

    
    private boolean retrieveVfcuData(Connection damsConn, Integer vfcuMediaFileId) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT    a.base_path_staging, a.file_path_ending, " +
                    "           b.vendor_checksum, " +
                    "           TO_CHAR(b.media_file_date,'YYYY-MM')  " + 
                    "FROM       vfcu_md5_file a, " +
                    "           vfcu_media_file b " +
                    "WHERE      a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                    "AND        b.vfcu_media_file_id = " + vfcuMediaFileId ;
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                this.pathBase = rs.getString(1);
                this.pathEnding = rs.getString(2);
                this.vendorChecksum = rs.getString(3);
                this.assetDate = rs.getString(4);
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
}
