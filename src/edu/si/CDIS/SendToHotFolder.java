/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISActivityLog;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import edu.si.CDIS.DAMS.StagedFile;
import java.io.File;
import java.util.Iterator;
import java.lang.Thread;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author rfeldman
 */
public class SendToHotFolder {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection cisConn;
    String cisSourceDB;
    
    LinkedHashMap <String,String> masterMediaIds; 
    
    private void masterMediaIds (String uniqueMediaId, String filename) {
        this.masterMediaIds.put(uniqueMediaId, filename); 
    }
    
    /*  Method :        processList
        Arguments:      
        Returns:      
        Description:    Goes through the list of images that were determined to go to DAMS.  To avoid duplicates,
                        we check DAMS for an already existing image before we choose to create a new one.
        RFeldman 3/2015
    */
    
    
    
    private void processList (CDIS cdis) {
               
        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair for insert into CDIS_MAP table       
        Iterator<String> it = masterMediaIds.keySet().iterator();
        
        while (it.hasNext())  {  
                
            String uniqueMediaId = it.next();
            
            //Make sure the last transaction is committed
            try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for uniqueMediaId: " + uniqueMediaId);
                
            CDISMap cdisMap = new CDISMap();                           
            cdisMap.setFileName(masterMediaIds.get(uniqueMediaId));
            cdisMap.setVfcuMediaFileId(Integer.parseInt(uniqueMediaId));
            
            // Now that we have the cisUniqueMediaId, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord(cdis);
                    
            if (!mapEntryCreated) {
                logger.log(Level.FINER, "Could not create CDISMAP entry, retrieving next row");

                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
                
            //Log into the activity table
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MIC");    
            boolean activityLogged = cdisActivity.insertActivity(damsConn);
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                 //Remove from the list of renditions to ingest, we dont want to bring this file over without an activity_log entry
                it.remove();
                
                //rollback the database to remove the map Entry
                try { if ( damsConn != null)  damsConn.rollback(); } catch (Exception e) { e.printStackTrace(); }
                
                continue;
            }
            
            StagedFile stagedFile = new StagedFile();
            // Get the child record
            String childMediaId = null;
            childMediaId = stagedFile.retrieveSubFileId(damsConn, uniqueMediaId);

            cdisMap = new CDISMap();
            String childFileName = FilenameUtils.getBaseName(masterMediaIds.get(uniqueMediaId)) + ".tif";
            cdisMap.setFileName(childFileName);
            cdisMap.setVfcuMediaFileId(Integer.parseInt(childMediaId));
            
            // put the entry into the CDIS_MAP table
            mapEntryCreated = cdisMap.createRecord(cdis);
            
             if (!mapEntryCreated) {
                logger.log(Level.FINER, "Could not create CDISMAP entry, retrieving next row");

                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
                
            //Log into the activity table
            cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MIC");    
            activityLogged = cdisActivity.insertActivity(damsConn);
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                 //Remove from the list of renditions to ingest, we dont want to bring this file over without an activity_log entry
                it.remove();
                
                //rollback the database to remove the map Entry
                try { if ( damsConn != null)  damsConn.rollback(); } catch (Exception e) { e.printStackTrace(); }
                
                continue;
            }
            
        }
        
        //Obtain empty hot folder to put these files into
        String hotFolderBaseName = null;
        File hotFolderBase;
        
        boolean lastHotFolder = false;
        int hotFolderIncrement = 1;
        
        String fullMasterHotFolderNm = null;
        File fullHotFolder = null;
        

        while (!lastHotFolder) {
        
            hotFolderBaseName = cdis.properties.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
            hotFolderBase = new File (hotFolderBaseName);
            
            
            if (hotFolderBase.exists()) {
                int numMasterFolderFiles = 0;
                int numSubFolderFiles = 0;
        
                //count files in hotfolder master area
                fullMasterHotFolderNm = hotFolderBaseName + "\\MASTER";
                fullHotFolder = new File(fullMasterHotFolderNm);
                
                if (fullHotFolder.exists()) {
                    numMasterFolderFiles = fullHotFolder.list().length;
                }
                else {
                    logger.log(Level.FINER, "Error, Could not find MASTER hotfolder directory in: ", fullMasterHotFolderNm);
                    break;
                }
                
                //count files in hotfolder subfiles area
                String fullSubFilesHotFolderNm = hotFolderBaseName + "\\SUBFILES";
                File subFilesHotFolderNm = new File(fullSubFilesHotFolderNm);
                
                if (subFilesHotFolderNm.exists()) {
                    numSubFolderFiles = subFilesHotFolderNm.list().length;
                }
                else {
                    logger.log(Level.FINER, "Error, Could not find SUBFILES hotfolder directory in: ", subFilesHotFolderNm);
                    break;
                }
                                
                if (numMasterFolderFiles + numSubFolderFiles == 0 ) {
                    //check the subfile too, just in case
                    // We found an empty hotfolder, use this one
                    break;
                }
                
            }
            else {
                if (hotFolderIncrement > 1) {
                    //we went trhough more than one hot folder, but this one is not found.
                    // thus this is the last hotfolder in a series. 
                    lastHotFolder = true;
                    try {
                        Thread.sleep(100000);
                        hotFolderIncrement = 1;
                    } catch (Exception e) {
                        logger.log(Level.FINER, "Exception in Thread.sleep: " + hotFolderBaseName);
                    }
                    
                }
                else {
                    logger.log(Level.FINER, "Unable to locate Hot Folder Main Directory: " + hotFolderBaseName);
                }
                        
            }
            
            hotFolderIncrement ++;
        }
        
        logger.log(Level.FINER, "Will use current hotfolder: " + hotFolderBaseName);
        
            
        //loop through the NotLinked Master Media and copy the files
        for (String masterMediaId : masterMediaIds.keySet()) {       
            
            try {
                
                CDISMap cdisMap = new CDISMap();
                cdisMap.setBatchNumber(cdis.batchNumber);
                StagedFile stagedFile = new StagedFile();
 
                String childMediaId = null;
            
                try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
                //get the subFile ID and populate FileName from the masterID first
                childMediaId = stagedFile.retrieveSubFileId(damsConn, masterMediaId);
                
                //Get the file path for the vfcu_id
                stagedFile.populateNamePathFromId(cdis.damsConn, Integer.parseInt(childMediaId));
                cdisMap.setVfcuMediaFileId(Integer.parseInt(childMediaId));
                cdisMap.populateIdFromVfcuId(damsConn);
                 
                //Find the image and move/copy to hotfolder
                boolean fileCopied = stagedFile.copyToSubfile(hotFolderBaseName); 
                
                if (! fileCopied) {
                    logger.log(Level.FINER, "Error, unable to copy file to Subfile");
                    continue;
                }
                
                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("FCS");
                boolean activityLogged = cdisActivity.insertActivity(damsConn);
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                
                
                //now send the master file to the hotfolder
                boolean fileMoved = stagedFile.populateNamePathFromId(cdis.damsConn, Integer.parseInt(masterMediaId));
                 if (! fileMoved) {
                    logger.log(Level.FINER, "Error, unable to move file to master");
                    continue;
                }
               
                //Get the CDIS_ID 
                cdisMap.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
                cdisMap.populateIdFromVfcuId(damsConn);
                stagedFile.moveToMaster(hotFolderBaseName);    
                
                cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());   
                cdisActivity.setCdisStatusCd("FMM");
                activityLogged = cdisActivity.insertActivity(damsConn);
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                
            } catch (Exception e) {
                //ErrorLog errorLog = new ErrorLog ();
                //errorLog.capture(cdisMap, "PLE", "File Copy Failure for FileName:" + mediaFileName  + " " + e, damsConn);   
                
            } finally {
                
                //make sure we commit the final time through the loop
                try { if (damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
            }
        }
        
        // if there are any files in the hotfolder now, create a ready.txt file
        if (fullHotFolder.exists()) {
            if (fullHotFolder.list().length > 0) {
                createReadyFile(hotFolderBaseName + "\\MASTER");
            }
        }
    }

    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of media IDs that need to be integrated into DAMS
        RFeldman 3/2015
    */
    private Integer populateNewMediaList (CDIS cdis) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        int numCisRenditionsFound = 0;

        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
              
            if (sqlTypeArr[0].equals("idListToSend")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
            sql = sql + "AND ROWNUM < " + cdis.properties.getProperty("maxNumFiles") + " + 1";
            
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                    switch (cisSourceDB) {
                        case "none" :
                             stmt = damsConn.prepareStatement(sql);
                             break;
                        case "TMSDB" :
                            stmt = cisConn.prepareStatement(sql);
                            break;
                            
                        default:     
                            logger.log(Level.SEVERE, "Error: Invalid ingest source {0}, returning", cisSourceDB );
                            return numCisRenditionsFound;
                    }
                                                    
                    rs = stmt.executeQuery();
                     
                    while (rs.next()) {           
                        masterMediaIds(rs.getString("uniqueMediaId"), rs.getString("mediafileName"));
                        numCisRenditionsFound ++;
                    }   

            } catch (Exception e) {
                    logger.log(Level.FINER, "Error: obtaining CIS ID list ", e );
                    return numCisRenditionsFound;
                    
            } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
            
        }
        
        return numCisRenditionsFound;
    }
    
    /*  Method :        ingest
        Arguments:      
        Returns:      
        Description:    The main entrypoint or 'driver' for the ingestToDams operation Type
        RFeldman 3/2015
    */
     public void ingest (CDIS cdis) {
                                                                                       
        this.damsConn = cdis.damsConn;
        this.cisSourceDB = cdis.properties.getProperty("cisSourceDB"); 
         
        if (! this.cisSourceDB.equals("none")) { 
            this.cisConn = cdis.cisConn;
        }
  
        this.masterMediaIds = new LinkedHashMap<String, String>();
        
        // Get the list of new Media to add to DAMS
        Integer numCisRenditions = populateNewMediaList (cdis);
        if  (! (numCisRenditions > 0 )) {
             logger.log(Level.FINER, "No Media Found to process in this batch.  Exiting");
             return;
        }
        
        // Process each media item from list (move to workfolder/hotfolder)
        logger.log(Level.FINER, "Processing media List");
        processList(cdis);
     }
     
    private void createReadyFile (String hotDirectoryName) {
    
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = hotDirectoryName + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }
    
    
}
