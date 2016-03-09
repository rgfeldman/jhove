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

import edu.si.CDIS.DAMS.Database.SiPreservationMetadata;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.DAMS.StagedFile;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.utilties.ErrorLog;


import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LinkToDAMS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String vendorChecksum;
    private String pathBase;
    private String pathEnding;
    private Integer vfcuMd5FileId;
    
    private void logIngestFailedFile (String filename) {
        logger.log(Level.FINER, "FailedFileName found: " + filename);
        
        //getCdisMapID for filename/status so we can log in table
        //may want to check by checksum too later to make sure we have uniqueness....but drawback is we want to verify file is not corrupted
        
        CDISMap cdisMap = new CDISMap();
        cdisMap.setFileName(filename);
        cdisMap.populateIdForNameNullUoiid();
                
        ErrorLog errorLog = new ErrorLog ();
        errorLog.capture(cdisMap, "LKD-DIPF", "Error, Record failed upon ingest");
    }
    
    private void checkForFailedFiles () {
        
        // look in all hot folder failed folders to check if there are files there, if there are, record them in error log
        boolean lastHotFolder = false;
        int hotFolderIncrement = 1;
        
        while (!lastHotFolder) {
        
            String hotFolderBaseName = CDIS.getProperty("failedFolderArea") + "_" + hotFolderIncrement;
            File hotFolderBase = new File (hotFolderBaseName);
         
            logger.log(Level.FINER, "hotFolderBaseName: " +  hotFolderBaseName);
            
            if (hotFolderBase.exists()) {
                int numFailedFiles = 0;

                //count files in failed folder master area
                String failedFolderNm = hotFolderBaseName + "\\FAILED";
                File failedHotFolder = new File(failedFolderNm);
                
                logger.log(Level.FINER, "FailedDir: " + failedFolderNm);
                
                if (failedHotFolder.exists()) {
                    String[] filenames = failedHotFolder.list();
                    
                    for (String filename : filenames) {
                        logIngestFailedFile(filename);
                        numFailedFiles ++;
                    }
                    
                }
                else {
                    logger.log(Level.FINER, "ERROR: Could not find Failed directory in: " + failedFolderNm);
                    break;
                }
                
                if (numFailedFiles == 0) {
                    //We have no failures
                    logger.log(Level.FINER, "There are no files in the FAILED directory " );
                }
                else {
                    logger.log(Level.FINER, "ERROR: found FAILED files in: " + failedFolderNm);
                    File[] listOfFiles = failedHotFolder.listFiles();
                    
                     for (int i = 0; i < listOfFiles.length; i++) {
                         logger.log(Level.FINER, "Failed File: " + listOfFiles[i].getName() );
                     }
                }
                
                //increment to the next hot folder in the series
                hotFolderIncrement ++;
                
            }
            else {
                //The failed folder in the series has been reached, the main hot folder directory does not exist
                lastHotFolder = true;
            }
        }
    }
    
    
    
    public void link () {
        
        // now find all the unlinked images in DAMS (uoiid is null)
        CDISMap cdisMap = new CDISMap();
        
        HashMap<Integer, String> unlinkedDamsRecords;
        unlinkedDamsRecords = new HashMap<> ();
        
        Set md5IdSet = new HashSet();
        
        unlinkedDamsRecords = cdisMap.returnUnlinkedMediaInDams();
                
        checkForFailedFiles();
        
        // See if we can find the media in DAMS based on the filename and checksum combination
        for (Integer key : unlinkedDamsRecords.keySet()) {  

            Uois uois = new Uois();
            cdisMap = new CDISMap();
            CDISActivityLog activityLog = new CDISActivityLog();
      
            cdisMap.setCdisMapId(key);
            cdisMap.setFileName(unlinkedDamsRecords.get(key));
            
            uois.setName(unlinkedDamsRecords.get(key));
            
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            
            // populate the cdis vfcuId
            boolean vfcuIdPopulated = cdisMap.populateVfcuId();
            if (! vfcuIdPopulated) {
                logger.log(Level.FINER, "ERROR: unable to get vfcuId for map_id: " + cdisMap.getCdisMapId());
                continue;
            }
            
            //with the vfcuId get the rest of the VFCU data including the paths and checksum info
            boolean vfcuInfoObtained = retrieveVfcuData(cdisMap.getVfcuMediaFileId());
            if (!vfcuInfoObtained) {
                logger.log(Level.FINER, "ERROR: Unable to obtain vfcu information for CDIS_MAP_ID: " + cdisMap.getCdisMapId());
                continue;
            } 
            
            //Add the md5Id to the set so we can check for completeness later
            md5IdSet.add(this.vfcuMd5FileId);
            
            //Get the uoiid for the name and checksum
            boolean uoiidFound = uois.populateUoiidForNameChksum(vendorChecksum);
            if (!uoiidFound) {
                logger.log(Level.FINER, "ERROR: No matches in DAMS for filename/checksum " + uois.getName() );
                continue;
            }
            
            cdisMap.setDamsUoiid(uois.getUoiid());
            
            boolean uoiidUpdated = cdisMap.updateUoiid();
            if (!uoiidUpdated) {
                 ErrorLog errorLog = new ErrorLog ();
                 errorLog.capture(cdisMap, "LKD-CUIF", "ERROR: unable to update UOIID in CDIS for uoiid: " + uois.getUoiid()  );
                 continue;
            }
                
            
                
            // Update the DAMS checksum information in preservation module
            SiPreservationMetadata siPreservation = new SiPreservationMetadata();
            siPreservation.setUoiid(cdisMap.getDamsUoiid()); 
            siPreservation.setPreservationIdNumber(this.vendorChecksum);
            boolean preservationInfoAdded = siPreservation.insertRow();
            if (! preservationInfoAdded) {
                 ErrorLog errorLog = new ErrorLog ();
                 errorLog.capture(cdisMap, "LKD-DPUF", "Error, unable to insert preservation data");
                 continue;
            }
            
            // Move any .tif files from staging to NMNH EMu pick up directory (on same DAMS Isilon cluster)           
            if ( (CDIS.getProperty("emuPickupLocation") != null) && (cdisMap.getFileName().endsWith("tif") ) ) {
                StagedFile stagedFile = new StagedFile();
                stagedFile.setBasePath(pathBase);
                stagedFile.setPathEnding(pathEnding);
                stagedFile.setFileName(cdisMap.getFileName());
            
                boolean subFileDelivered = stagedFile.deliverSubFile(CDIS.getProperty("emuPickupLocation"));
                if (! subFileDelivered) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "LKD-SEPF", "Error, unable to move file to emu pickup location");
                    continue;
                }
                
                activityLog.setCdisStatusCd("FME");
                activityLog.insertActivity();
            }
            
            activityLog.setCdisStatusCd("LDC");
            activityLog.insertActivity();
            
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
        }
        
        if (CDIS.getProperty("emuPickupLocation") != null) {
            // Get the list of vendor directories that may have been processed in this batch
            Iterator md5Id = md5IdSet.iterator();
        
            while (md5Id.hasNext()) { 
                int totalFilesDb = 0;
                int totalFilesFileSystem = 0;
            
                //count the number of files in batch not yet completed
                this.vfcuMd5FileId = (Integer) md5Id.next();
                //getDirectory Name ending for this md5Id
                setFilePathEndingForId();
            
                if (! this.pathEnding.endsWith("tifs")) {
                    //we ignore the pickup location, only interested in tifs for delivery to emu
                    continue;
                }
            
                String emuPickupLocation = CDIS.getProperty("emuPickupLocation") + "\\" + this.pathEnding;
            
            
                int numUnprocessedFiles = countUnprocessedFiles();
            
                //if the number of files in batch not yet completed > 1 then it is not yet ready, go grab the next one
                if (numUnprocessedFiles > 0 ) {
                    continue;
                }
                    
                //count the number of files in batch total
                totalFilesDb = countFilesVfcuDBVendorDir();
            
                //count the number of files in vendor directory
                totalFilesFileSystem = countEmuFiles(emuPickupLocation);
            
                logger.log(Level.FINEST,"Files In DB for VendorDir! " + totalFilesDb); 
                logger.log(Level.FINEST,"FilesInEmuFileSystem! " + totalFilesFileSystem); 
                
                //if number of files is the same then create emu ready file
                if ((totalFilesDb > 0 )&& (totalFilesDb == totalFilesFileSystem)) {
                
                    createEmuReadyFile(emuPickupLocation);
                }
                else {
                    logger.log(Level.FINEST,"Need to wait, more files to process! "); 
                }
            }
        }
    }

    
    private boolean retrieveVfcuData(Integer vfcuMediaFileId) {
  
        String sql = "SELECT    a.vfcu_md5_file_id, a.base_path_staging, a.file_path_ending, " +
                    "           b.vendor_checksum " +
                    "FROM       vfcu_md5_file a, " +
                    "           vfcu_media_file b " +
                    "WHERE      a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                    "AND        b.vfcu_media_file_id = " + vfcuMediaFileId ;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                this.vfcuMd5FileId = rs.getInt(1);
                this.pathBase = rs.getString(2);
                this.pathEnding = rs.getString(3);
                this.vendorChecksum = rs.getString(4);
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    private void createEmuReadyFile (String emuPickupDirName) {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = emuPickupDirName + "\\EMu_ready.txt";

                logger.log(Level.FINER, "Creating EMu ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create EMu_ready.txt file",e);;
        }
    }   
    
    private int countUnprocessedFiles () {
        
        int count = 0;
        
        String sql =   "SELECT count (*) " + 
                       "FROM   vfcu_media_file a " +
                       "WHERE a.vfcu_md5_file_id = " + this.vfcuMd5FileId +
                       " AND    NOT EXISTS ( " +
                       "    SELECT  'X' " +
                       "    FROM    cdis_map b, " +
                       "            cdis_activity_log c " +
                       "WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                       "AND  b.cdis_map_id = c.cdis_map_id " +
                       "AND c.cdis_status_cd = 'LDC' )"; 
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                count = rs.getInt(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain Count of unprocessed files", e );
        }
        
        return count;
    }
    
    private int countFilesVfcuDBVendorDir () {
        
        int count = 0;
        
        String sql =    "SELECT count (*) " +
                        "FROM   vfcu_media_file a " +
                        "WHERE  a.vfcu_md5_file_id = " + this.vfcuMd5FileId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                count = rs.getInt(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain Count in VFCU_batch_id", e );
        }
        
        return count;
    }
        
    private int countEmuFiles (String emuLocation) {
        int count = 0;
        
        File emuDirectory = new File(emuLocation);
        
        for (File file : emuDirectory.listFiles()) {
            if (file.getName().endsWith("tif")) {
                count++;
            }
        }

        return count;
        
    }    
    
    private void setFilePathEndingForId () {
        
        String sql =    "SELECT file_path_ending " +
                        "FROM   vfcu_md5_file " +
                        "WHERE  vfcu_md5_file_id = " + this.vfcuMd5FileId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                this.pathEnding = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain path ending", e );
        }
        
    }
    
}
