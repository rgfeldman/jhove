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
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.CDIS.DAMS.StagedFile;
import edu.si.CDIS.DAMS.TempXfer;
import java.util.Iterator;

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
        }
            
        //loop through the NotLinked Master Media and copy the files
        for (String masterMediaId : masterMediaIds.keySet()) {       
            
            try {
                
                CDISMap cdisMap = new CDISMap();
                StagedFile stagedFile = new StagedFile();
 
                String childMediaId = null;
                
                String masterMediaFileName = masterMediaIds.get(masterMediaId);
            
                try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
                //get the subFile ID and populate FileName from the masterID first
                childMediaId = stagedFile.retrieveSubFileInfo(damsConn, masterMediaId);
                
                //Get the file path for the vfcu_id
                stagedFile.populatePathFromVfcuId(cdis.damsConn, Integer.parseInt(childMediaId));
                String sendType = null;

                //Find the image and move/copy to hotfolder
                sendType = stagedFile.sendToHotFolder(cdis); 
                
                
                /*
                cdisMap.setFileName(masterMediaFileName);
                cdisMap.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
                */
                
                //Get the CDIS_ID that was inserted in the previous loop
                cdisMap.populateIdFromVfcuId(damsConn);
                    
                

                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd(sendType);         
                boolean activityLogged = cdisActivity.insertActivity(damsConn);
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
        }
        
        // Process each media item from list (move to workfolder/hotfolder)
        logger.log(Level.FINER, "Processing media List");
        processList(cdis);
        
        /*
        HotIngestFolder hotFolder = new HotIngestFolder();
        hotFolder.populateDistinctList(damsConn, cdis.getBatchNumber());
        hotFolder.setBaseDir(cdis.properties.getProperty("hotFolderBaseDir"));
        
        if(hotFolder.distinctHotFolders.isEmpty()) {
            logger.log(Level.FINER, "Error: Unable to get HotFolder from database, returning ");
            return;
        }
        
        // Check to see if anything in any of the workfolder directories.
        // We only need to continue if there is a file there.
        Integer numWorkFiles = hotFolder.countIngestFiles(cdis.getBatchNumber(), "workFolder");
        if (! (numWorkFiles > 0)) {
            logger.log(Level.FINER, "No work Files detected to process, returning");
            return;
        }
        
        // Check to see if anything in any of the hotfolder directories.
        // We pause if there is a file in the hotfolder and wait for the hotfolder file to be ingested.
        
        logger.log(Level.FINER, "{0} Workfiles detected.  Need to check if ANY hotfiles exist from another process before continuing", numWorkFiles);
        while (hotFolder.countIngestFiles(cdis.getBatchNumber(), "master") > 0) {
 
            logger.log(Level.FINER, "hotFolder Master Directory is not empty.  Check back in 5 minutes");
                
            try {
                Thread.sleep(300000);
            } catch (Exception e) {
                logger.log(Level.FINER, "Exception in sleep ", e);
            }
        }
        
        //Move from workfolder to hotfolder
        
        hotFolder.sendTo(damsConn, cdis.getBatchNumber());
         */
        
     }
}
