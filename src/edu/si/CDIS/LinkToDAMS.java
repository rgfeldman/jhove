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
import edu.si.CDIS.Database.CdisCisMediaTypeR;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.DAMS.MediaRecord;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.Utils.XmlSqlConfig;


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
    HashMap <Integer, String> neverLinkedDamsIds; 
    Set md5IdSet;
    
    private void logIngestFailedFile (String filename) {
        logger.log(Level.FINER, "FailedFileName found: " + filename);
        
        //getCdisMapID for filename/status so we can log in table
        //may want to check by checksum too later to make sure we have uniqueness....but drawback is we want to verify file is not corrupted
        
        CDISMap cdisMap = new CDISMap();
        cdisMap.setFileName(filename);
        cdisMap.populateIdForNameNullUoiid();
                
        ErrorLog errorLog = new ErrorLog ();
        errorLog.capture(cdisMap, "INGDAM", "Error, Record failed upon ingest");
    }
    
    private void checkForFailedFiles () {
        
        // look in all hot folder failed folders to check if there are files there, if there are, record them in error log
        boolean lastHotFolder = false;
        int failedFolderIncrement = 1;
        
        while (!lastHotFolder) {
        
            String failedFolderBaseName = CDIS.getProperty("failedFolderArea") + "_" + failedFolderIncrement;
            File failedFolderBase = new File (failedFolderBaseName);
         
            logger.log(Level.FINER, "hotFolderBaseName: " +  failedFolderBaseName);
            
            if (failedFolderBase.exists()) {
                int numFailedFiles = 0;

                //count files in failed folder master area
                String failedFolderNm = failedFolderBaseName + "\\FAILED";
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
                failedFolderIncrement ++;
                
            }
            else {
                //The failed folder in the series has been reached, the main hot folder directory does not exist
                lastHotFolder = true;
            }
        }
    }
    
    private boolean populateUnlinkedMedia () {
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("DamsSelectList"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }       
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                    neverLinkedDamsIds.put(rs.getInt(1),rs.getString(2));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error, unable to obtain list of UOI_IDs to integrate", e);
                return false;
            }
        }
        
        return true;
        
    }
    
    public void link () {
        
        this.neverLinkedDamsIds = new HashMap <>();
        
        md5IdSet = new HashSet();
        
        checkForFailedFiles();
        
        boolean unlinkedListPopulated = populateUnlinkedMedia();
        
        // See if we can find the media in DAMS based on the filename and checksum combination
        for (Integer cdisMapId : neverLinkedDamsIds.keySet()) {

            Uois uois = new Uois();
            CDISMap cdisMap = new CDISMap();
            CDISActivityLog activityLog = new CDISActivityLog();
      
            cdisMap.setCdisMapId(cdisMapId);
            cdisMap.setFileName(neverLinkedDamsIds.get(cdisMapId));
            
            uois.setName(neverLinkedDamsIds.get(cdisMapId));
            
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            
            // populate the cdis vfcuId
            boolean vfcuIdPopulated = cdisMap.populateVfcuMediaFileId();
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
                 errorLog.capture(cdisMap, "UPDAMI", "ERROR: unable to update UOIID in CDIS for uoiid: " + uois.getUoiid()  );
                 continue;
            }
                
            // Update the DAMS checksum information in preservation module
            SiPreservationMetadata siPreservation = new SiPreservationMetadata();
            siPreservation.setUoiid(cdisMap.getDamsUoiid()); 
            siPreservation.setPreservationIdNumber(this.vendorChecksum);
            boolean preservationInfoAdded = siPreservation.insertRow();
            if (! preservationInfoAdded) {
                 ErrorLog errorLog = new ErrorLog ();
                 errorLog.capture(cdisMap, "UPDAMP", "Error, unable to insert preservation data");
                 continue;
            }
            
            //Check to see if we have to move this filetype to the post-ingest delivery area
            cdisMap.populateCdisCisMediaTypeId();
            CdisCisMediaTypeR cdisCisMediaTypeR = new CdisCisMediaTypeR();
            cdisCisMediaTypeR.setCdisCisMediaTypeId(cdisMap.getCdisCisMediaTypeId());
            cdisCisMediaTypeR.populatePostIngestDelivery();
            
            if (cdisCisMediaTypeR.getPostIngestDelivery().equals("Y")) {
                StagedFile stagedFile = new StagedFile();
                stagedFile.setBasePath(pathBase);
                stagedFile.setPathEnding(pathEnding);
                stagedFile.setFileName(cdisMap.getFileName());
            
                boolean subFileDelivered = stagedFile.deliverSubFile(CDIS.getProperty("postIngestDeliveryLoc"));
                if (! subFileDelivered) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "CPDELP", "Error, unable to move file to pickup location");
                    continue;
                }
                
                activityLog.setCdisStatusCd("FME");
                activityLog.insertActivity();
            }
            
            if ( (CDIS.getProperty("linkHierarchyInDams") != null ) && (CDIS.getProperty("linkHierarchyInDams").equals("true") ) ) {
                MediaRecord mediaRecord = new MediaRecord();
                
                mediaRecord.establishParentChildLink(cdisMap);
            }
            
            activityLog.setCdisStatusCd("LDC");
            activityLog.insertActivity();
            
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
        }
        
        if (CDIS.getProperty("postIngestDeliveryLoc") != null) {
            generateReadyFile ();
        }
    }
    
    
    private boolean generateReadyFile () {
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
                //we ignore the pickup location, only interested in tifs for delivery
                continue;
            }
            
            String postIngestDeliveryLoc = CDIS.getProperty("postIngestDeliveryLoc") + "\\" + this.pathEnding;
            
            
            int numUnprocessedFiles = countUnprocessedFiles();
            
            //if the number of files in batch not yet completed > 1 then it is not yet ready, go grab the next one
            if (numUnprocessedFiles > 0 ) {
                logger.log(Level.FINEST,"Batch not completed, number of unprocessed Files " + numUnprocessedFiles); 
                continue;
            }
                    
            //count the number of files in batch total
            totalFilesDb = countFilesVfcuDBVendorDir();
            
            //count the number of files in vendor directory
            totalFilesFileSystem = countDeliveredFiles(postIngestDeliveryLoc);
            
            logger.log(Level.FINEST,"Files In DB for VendorDir! " + totalFilesDb); 
            logger.log(Level.FINEST,"FilesInDeliveryFileSystem! " + totalFilesFileSystem); 
                
            //if number of files is the same then create ready file
            if ((totalFilesDb > 0 )&& (totalFilesDb == totalFilesFileSystem)) {
                   
                createReadyFile(postIngestDeliveryLoc);
            }
            else {
               logger.log(Level.FINEST,"Need to wait, more files to process! "); 
            }
            
        }
        return true;
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
    
    private void createReadyFile (String postIngestDeliveryLoc) {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = postIngestDeliveryLoc + "\\" + CDIS.getProperty("readyFileName");

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready file",e);;
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
        
    private int countDeliveredFiles (String postIngestDeliveryLoc) {
        int count = 0;
        
        File deliveryDirectory = new File(postIngestDeliveryLoc);
        
        for (File file : deliveryDirectory.listFiles()) {
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
