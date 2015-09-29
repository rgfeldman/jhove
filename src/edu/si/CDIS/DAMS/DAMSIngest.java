/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.CDISActivityLog;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import edu.si.CDIS.utilties.ErrorLog;

/**
 *
 * @author rfeldman
 */
public class DAMSIngest {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection cisConn;
    String cisSourceDB;
    String hotFolderBaseDir;
    public ArrayList<String> distinctHotFolders;
    String folderLetterExtension;
    
    LinkedHashMap <String,String> renditionsForDAMS; 
    
    private void addRenditionsForDAMS (String cisUniqueMediaId, String filename) {
        this.renditionsForDAMS.put(cisUniqueMediaId, filename); 
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
        for (String cisUniqueMediaId : renditionsForDAMS.keySet()) {
                
            //Make sure the last transaction is committed
            try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for cisUniqueMediaId: " + cisUniqueMediaId);
                
            CDISMap cdisMap = new CDISMap();
            CDISActivityLog cdisActivity = new CDISActivityLog();
                
            String cisFileName = renditionsForDAMS.get(cisUniqueMediaId);
                                
            // Now that we have the cisUniqueMediaId, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord(cdis, cisUniqueMediaId, cisFileName);
                    
            if (!mapEntryCreated) {
                logger.log(Level.FINER, "Could not create CDISMAP entry, retrieving next row");
                continue;
            }
                
            //Log into the activity table
            boolean activityLogged = cdisActivity.insertActivity(damsConn, cdisMap.getCdisMapId(), "MI");
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                continue;
            }
        }
            
        //loop through the NotLinked RenditionList and copy the files
        for (String cisUniqueMediaId : renditionsForDAMS.keySet()) {       
                
            CDISMap cdisMap = new CDISMap();
            String cisFileName = renditionsForDAMS.get(cisUniqueMediaId);
            
            try {
                
                try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
                cdisMap.setBatchNumber(cdis.getBatchNumber());
                cdisMap.setFileName(cisFileName);
                cdisMap.populateIDForFileBatch(damsConn);
                    
                boolean sentForIngest = false;
                    
                MediaFile mediaFile = new MediaFile();
                    
                //Find the image on the media drive
                sentForIngest = mediaFile.sendToIngest(cdis, cisFileName, cisUniqueMediaId, cdisMap);   

                    
                // If we have no error condition, mark status in activity table, else flag as error
                if (! sentForIngest) {
                    //We should have logged the error.  Pull the next record
                    continue;
                }
                    
                CDISActivityLog cdisActivity = new CDISActivityLog();
                //Log into the activity table
                boolean activityLogged = cdisActivity.insertActivity(damsConn, cdisMap.getCdisMapId(), "SW");
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }

               
                
            } catch (Exception e) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "PLE", "File Copy Failure for FileName:" + cisFileName  + " " + e, damsConn);   
                
            } finally {
                
                //make sure we commit the final time through the loop
                try { if (damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
            }
        }
    
    }

    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of CIS IDs that need to be integrated into DAMS
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
              
            if (sqlTypeArr[0].equals("cisSelectList")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
            sql = sql + "AND ROWNUM < " + cdis.properties.getProperty("maxNumFiles") + " + 1";
            
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                    switch (cisSourceDB) {
                        case "CDISDB" :
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
                        addRenditionsForDAMS(rs.getString("cisUniqueMediaId"), rs.getString("fileName"));
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
    
    /*  Method :        moveFilesToHotFolder
        Arguments:      
        Returns:      
        Description:    Moves media files from the workforder to the hotfolder location specified in the config file
        RFeldman 3/2015
    */
    
    
    private void moveFilesToHotFolder (Long batchNumber) {
          
        Integer numFilesinCurrentFolder;
        
        // We need to look in all workFolders for this batch 
        for(Iterator<String> iter = distinctHotFolders.iterator(); iter.hasNext();) {
            
            numFilesinCurrentFolder = 0;
            
            String currentMainDirName = iter.next();
            
            //Set the hotfolder letter extension for multi-processing.
            //First we start with the letter 'a' then increment
            folderLetterExtension = "a";

            // Check to see if hotfolder with letter extension has data in it
            // If it does, then increment the letter extension
                               
            Integer numFiles;
            boolean firstPass = true;
            File currentHotFolderMaster;
            
            do {
                
                currentHotFolderMaster = new File (hotFolderBaseDir + "\\" + currentMainDirName + folderLetterExtension + "\\MASTER"); 
                numFiles = countFilesInIngestFolder (batchNumber, "master");
               
                if (numFiles == -1 ) {
                    //The direcotry does not exist
                    if (firstPass) {
                        //invalid, hot folder does not exist. Return
                        logger.log(Level.FINER, "Invalid HotFolderName, cannot ingest: " + currentHotFolderMaster.getAbsolutePath());
                        return;
                    }
                    else {
                        //HotFolder extension does not exist...but previous ones did.
                        //Set to "a" and start back at beginning
                        logger.log(Level.FINER, "hotFolder Master Directory is not empty.  Will sleep and check back in 5 minutes");
                
                        try {
                            Thread.sleep(300000);
                        } catch (Exception e) {
                                logger.log(Level.FINER, "Exception in sleep ", e);
                        }
            
                        folderLetterExtension = "a"; 
                        
                    }
                }
                else if ( numFiles > 0 ) {
                    int charValue = folderLetterExtension.charAt(0);
                    folderLetterExtension = String.valueOf( (char) (charValue + 1));
                    firstPass = false;
                }   

            } while (numFiles != 0);
               

            String currentHotFolderMasterName = currentHotFolderMaster.getAbsolutePath();
            logger.log(Level.FINER, "Current hotfolder set to : " + currentHotFolderMasterName);
            
            //Set the Workfile vatiable information
            String batchWorkFileDirName = hotFolderBaseDir + "\\" + currentMainDirName + "a\\TEMP-XFER\\" + batchNumber;
            File batchWorkFileDir = new File(batchWorkFileDirName);
            logger.log(Level.FINER, "Current workfolder set to : " + batchWorkFileDirName);
            
            // For each file in current work folder, move to the hot folder MASTER directory
            File[] filesForDams = batchWorkFileDir.listFiles();
            
            for(int i = 0; i < filesForDams.length; i++) {
            
                //Get MapID for logging and error capturing
                CDISMap cdisMap =  new CDISMap();
                cdisMap.setBatchNumber(batchNumber);
                cdisMap.setFileName(filesForDams[i].getName()); 
                cdisMap.populateIDForFileBatch(this.damsConn);
                
                try {
                    
                    //skip the file if named Thumbs.db
                    if (cdisMap.getFileName().equals("Thumbs.db")) {
                        continue;
                    }
                    
                    File fileForDams = filesForDams[i];
                     
                    if (! currentHotFolderMaster.exists()) {
                        logger.log(Level.FINER, "Error, hot folder not found : " + currentHotFolderMasterName);
                        continue;
                    }
                    FileUtils.moveFileToDirectory(fileForDams, currentHotFolderMaster, false);
                     
                    CDISActivityLog cdisActivity = new CDISActivityLog();
                    
                    boolean activityLogged = cdisActivity.insertActivity(damsConn, cdisMap.getCdisMapId(), "SH");
                    if (!activityLogged) {
                            logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                            continue;
                    } 
                    
                    //if we got this far we have succeeeded, increment count of files in hotfolder...
                    numFilesinCurrentFolder ++;
                    
                } catch (Exception e) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "MFH", "Move to Hotfolder MASTER Failure for FileName:" + cdisMap.getFileName()  + " " + e, damsConn );
                } 
            }
                         
            // If the processing was successful, files have been moved out and the directory will be empty
            // delete the workfolder subdirectory if it is empty 
            filesForDams = batchWorkFileDir.listFiles();
            if (! (filesForDams.length > 0)) { 
                logger.log(Level.FINER, "Deleting empty work file");
                batchWorkFileDir.delete();
            }  
            
        if (numFilesinCurrentFolder > 0) {
            //create ready file in current hotfolder    
            createReadyFile(currentHotFolderMasterName);        }
        }
    }
    
   /*  Method :        createReadyFile
        Arguments:      
        Returns:      
        Description:    Creates empty file named 'ready.txt' in hot folder.
                        This file indicates for the DAMS to create images based on the files in the hotfolder location
        RFeldman 3/2015
    */
    
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
    
    
    private boolean populateDistinctHotFolderList(Long batchNumber) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        this.distinctHotFolders = new ArrayList<String>();
        
        String sql = "SELECT    distinct a.hot_folder " + 
                    "FROM       cdis_for_ingest a, " +
                    "           cdis_map b " +
                    "WHERE  a.cis_unique_media_id = b.cis_unique_media_id " +
                    "AND    a.si_holding_unit = b.si_holding_unit " +
                    "AND    batch_number = " + batchNumber;
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs != null && rs.next()) {
                distinctHotFolders.add(rs.getString(1));
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
    
    private Integer countFilesInIngestFolder(Long batchNumber, String folderType) {
    
        // Check to see if anything is in hotfolder.  If there is, we need to wait for hotfolder to clear or we can run the risk of 
        // having files being ingested that are partially there.
            
        Integer numFiles = 0;
        
        for(Iterator<String> iter = distinctHotFolders.iterator(); iter.hasNext();) {
            
            String folderLocation = null;
            
            if (folderType.equals("workFolder")) {
                folderLocation = hotFolderBaseDir + "\\" + iter.next() + "a\\TEMP-XFER\\" + batchNumber;
            }
            else if (folderType.equals("master")) {
                folderLocation = hotFolderBaseDir + "\\" + iter.next() + this.folderLetterExtension + "\\MASTER";
            }
            else {
                logger.log(Level.FINER, "Error, invalid folder type");
                return -1;
            }
            
            File hotFolderSubDir = new File(folderLocation);
            
            if(hotFolderSubDir.isDirectory()){ 
                numFiles += hotFolderSubDir.list().length;
                logger.log(Level.FINER, "Directory: " + folderLocation + " Files: " + numFiles);
            }
            else {
                logger.log(Level.FINER, "Error, directory does not exist: " + folderLocation);
                return -1;
            }
        }
        
        return numFiles;
        
    }
    
    /*  Method :        ingest
        Arguments:      
        Returns:      
        Description:    The main entrypoint or 'driver' for the ingestToDams operation Type
        RFeldman 3/2015
    */
     public void ingest (CDIS cdis) {
                                                                                       
        this.damsConn = cdis.damsConn;
        this.hotFolderBaseDir = cdis.properties.getProperty("hotFolderBaseDir");
        this.cisSourceDB = cdis.properties.getProperty("cisSourceDB"); 
         
        if (! this.cisSourceDB.equals("CDISDB")) { 
            this.cisConn = cdis.cisConn;
        }
  
        this.renditionsForDAMS = new LinkedHashMap<String, String>();
        
        // Get the list of new Media to add to DAMS
        Integer numCisRenditions = populateNewMediaList (cdis);
        if  (! (numCisRenditions > 0 )) {
             logger.log(Level.FINER, "No Renditions Found to process in this batch.  Exiting");
        }
        
        // check if the renditions are in dams...and process each one in list (move to workfolder
        logger.log(Level.FINER, "Processing media List");
        processList(cdis);
        
        populateDistinctHotFolderList(cdis.getBatchNumber());
        
        if(distinctHotFolders.isEmpty()) {
            logger.log(Level.FINER, "Error: Unable to get HotFolder from database, returning ");
            return;
        }
        
        // Check to see if anything in any of the workfolder directories.
        // We only need to continue if there is a file there.
        Integer numWorkFiles = countFilesInIngestFolder(cdis.getBatchNumber(), "workFolder");
        if (! (numWorkFiles > 0)) {
            logger.log(Level.FINER, "No work Files detected to process, returning");
            return;
        }
        
        // Check to see if anything in any of the hotfolder directories.
        // We pause if there is a file in the hotfolder and wait for the hotfolder file to be ingested.
        
        logger.log(Level.FINER, "{0} Workfiles detected.  Need to check if ANY hotfiles exist from another process before continuing", numWorkFiles);
        while (countFilesInIngestFolder(cdis.getBatchNumber(), "master") > 0) {
 
            logger.log(Level.FINER, "hotFolder Master Directory is not empty.  Check back in 5 minutes");
                
            try {
                Thread.sleep(300000);
            } catch (Exception e) {
                logger.log(Level.FINER, "Exception in sleep ", e);
            }
        }
        
        //Move from workfolder to hotfolder
        moveFilesToHotFolder(cdis.getBatchNumber());
         
     }
}
