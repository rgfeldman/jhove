/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.VFCUMediaFile;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import edu.si.CDIS.DAMS.StagedFile;
import java.io.File;
import java.util.Iterator;
import edu.si.CDIS.utilties.ErrorLog;

/**
 *
 * @author rfeldman
 */
public class SendToHotFolder {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    private String cisSourceDB;
    
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
    
    
    
    private void processList () {
               
        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair for insert into CDIS_MAP table       
        Iterator<String> it = masterMediaIds.keySet().iterator();
        
        while (it.hasNext())  {  
                
            String uniqueMediaId = it.next();
            
            //Make sure the last transaction is committed
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for uniqueMediaId: " + uniqueMediaId);
                
            CDISMap cdisMap = new CDISMap();                           
            cdisMap.setFileName(masterMediaIds.get(uniqueMediaId));
            cdisMap.setVfcuMediaFileId(Integer.parseInt(uniqueMediaId));
            cdisMap.setCdisCisMediaTypeId(2);
            
            // Now that we have the cisUniqueMediaId, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord();
                    
            if (!mapEntryCreated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "SDH-CMIF", "Could not create CDISMAP entry, retrieving next row");
                    
                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
                
            //Log into the activity table
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MIC");    
            boolean activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                 //Remove from the list of renditions to ingest, we dont want to bring this file over without an activity_log entry
                it.remove();
                
                //rollback the database to remove the map Entry
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().rollback(); } catch (Exception e) { e.printStackTrace(); }
                
                continue;
            }
            
            VFCUMediaFile vfcuMediafile = new VFCUMediaFile();
            vfcuMediafile.setVfcuMediaFileId(cdisMap.getVfcuMediaFileId());
            
            // Get the child record
            int childVfcuMediaFileId = vfcuMediafile.retrieveSubFileId();
            if (! (childVfcuMediaFileId > 0 )) {
                logger.log(Level.FINER, "Could not get child ID");
                continue;
            }
            vfcuMediafile.setVfcuMediaFileId(childVfcuMediaFileId);

            vfcuMediafile.populateMediaFileName();
                    
            cdisMap = new CDISMap();

            cdisMap.setFileName(vfcuMediafile.getMediaFileName());
            cdisMap.setVfcuMediaFileId(vfcuMediafile.getVfcuMediaFileId());
            cdisMap.setCdisCisMediaTypeId(1);
            
            // put the entry into the CDIS_MAP table
            mapEntryCreated = cdisMap.createRecord();
            
             if (!mapEntryCreated) { 
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "SDH-CMIF", "Could not create CDISMAP entry, retrieving next row");
                    
                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
                
            //Log into the activity table
            cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MIC");    
            activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                 //Remove from the list of renditions to ingest, we dont want to bring this file over without an activity_log entry
                it.remove();
                
                //rollback the database to remove the map Entry
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().rollback(); } catch (Exception e) { e.printStackTrace(); }
                
                continue;
            }
        }
        
        //Obtain empty hot folder to put these files into
        String hotFolderBaseName = null;
        File hotFolderBase;
        
        String fullMasterHotFolderNm = null;
        File fullHotFolder = null;
        

        for (int hotFolderIncrement = 1;; hotFolderIncrement ++) {
        
            hotFolderBaseName = CDIS.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
            hotFolderBase = new File (hotFolderBaseName);
            
            if (hotFolderIncrement >  Integer.parseInt(CDIS.getProperty("maxHotFolderIncrement")) ) {
                
                try {
                    Thread.sleep(300000);  //sleep 5 minutes
                    hotFolderIncrement = 1;
                    logger.log(Level.FINER, "Sleeping, waiting for available hot folder...");
                    continue;
                } catch (Exception e) {
                    logger.log(Level.FINER, "Exception in Thread.sleep: " + hotFolderBaseName);
                }               
            }
            
            if (! hotFolderBase.exists()) {
                logger.log(Level.FINER, "Unable to locate Hot Folder Main Directory: " + hotFolderBaseName);
                return;
            }
        
            //count files in hotfolder master area
            fullMasterHotFolderNm = hotFolderBaseName + "\\MASTER";
            fullHotFolder = new File(fullMasterHotFolderNm);
                
            if (! fullHotFolder.exists()) {
                logger.log(Level.FINER, "Error, Could not find MASTER hotfolder directory in: ", fullMasterHotFolderNm);
                return;
            }
            int numMasterFolderFiles = fullHotFolder.list().length;
            
            //count files in hotfolder subfiles area
            String fullSubFilesHotFolderNm = hotFolderBaseName + "\\SUBFILES";
            File subFilesHotFolderNm = new File(fullSubFilesHotFolderNm);
                
            if (! subFilesHotFolderNm.exists()) {
                logger.log(Level.FINER, "Error, Could not find SUBFILES hotfolder directory in: ", subFilesHotFolderNm);
                return;
            }
            int numSubFolderFiles = subFilesHotFolderNm.list().length;
                    
            if (numMasterFolderFiles + numSubFolderFiles == 0 ) {
                 // We found an empty hotfolder, use this one
                break;
            }
                
        }
        
        logger.log(Level.FINER, "Will use current hotfolder: " + hotFolderBaseName);
        
            
        //loop through the NotLinked Master Media and copy the files
        for (String masterMediaId : masterMediaIds.keySet()) {       
            
            try {
                
                CDISMap cdisMap = new CDISMap();
                cdisMap.setBatchNumber(CDIS.getBatchNumber());
                StagedFile stagedFile = new StagedFile();
            
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
                VFCUMediaFile vfcuMediafile = new VFCUMediaFile();
                vfcuMediafile.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
            
                // Get the child record
                int childVfcuMediaFileId = vfcuMediafile.retrieveSubFileId();
                if (! (childVfcuMediaFileId > 0 )) {
                    logger.log(Level.FINER, "Could not get child ID");
                    continue;
                }
                
                //Get the file path for the vfcu_id
                boolean infoPopulated = stagedFile.populateNamePathFromId(childVfcuMediaFileId);
                if (! infoPopulated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "SDH-FCSF", "Error, unable to populate name and path from database for subfile ");
                    continue;
                }
                
                cdisMap.setVfcuMediaFileId(childVfcuMediaFileId);
                cdisMap.populateIdFromVfcuId();
                 
                //Find the image and move/copy to hotfolder
                boolean fileCopied = stagedFile.copyToSubfile(hotFolderBaseName);   
                if (! fileCopied) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "SDH-FCSF", "Error, unable to copy file to subfile: " + stagedFile.getFileName());
                    continue;
                }
                
                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("FCS");
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                
                
                //now send the master file to the hotfolder
                infoPopulated = stagedFile.populateNamePathFromId(Integer.parseInt(masterMediaId));
                if (! infoPopulated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "SDH-FMMF", "Error, unable to populate name and path from database for master file ");
                    continue;
                }
               
                //Get the CDIS_ID 
                cdisMap.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
                cdisMap.populateIdFromVfcuId();
                boolean fileMoved = stagedFile.moveToMaster(hotFolderBaseName);  
                if (! fileMoved) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "SDH-FMMF", "Error, unable to move file to master: " + stagedFile.getFileName());
                    continue;
                }
                
                cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());   
                cdisActivity.setCdisStatusCd("FMM");
                activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                
            }  finally {
                
                //make sure we commit the final time through the loop
                try { if (CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
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
    private Integer populateNewMediaList () {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        int numCisRenditionsFound = 0;

        String sql = null;
        String sqlTypeArr[];
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
              
            if (sqlTypeArr[0].equals("idListToSend")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
            sql = sql + "AND ROWNUM < " + CDIS.getProperty("maxNumFiles") + " + 1";
            
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                stmt = CDIS.getDamsConn().prepareStatement(sql);
                                                    
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
     public void ingest () {
                                                                                       
        this.cisSourceDB = CDIS.getProperty("cisSourceDB"); 
  
        this.masterMediaIds = new LinkedHashMap<>();
        
        // Get the list of new Media to add to DAMS
        Integer numCisRenditions = populateNewMediaList ();
        if  (! (numCisRenditions > 0 )) {
             logger.log(Level.FINER, "No Media Found to process in this batch.  Exiting");
             return;
        }
        
        // Process each media item from list (move to workfolder/hotfolder)
        logger.log(Level.FINER, "Processing media List");
        processList();
     }
     
    private void createReadyFile (String hotDirectoryName) {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = hotDirectoryName + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }   
}
