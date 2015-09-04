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
    
    LinkedHashMap <String,String> renditionsForDAMS; 
    
    private void addRenditionsForDAMS (String cisID, String filename) {
        this.renditionsForDAMS.put(cisID, filename); 
    }
    
    /*  Method :        processList
        Arguments:      
        Returns:      
        Description:    Goes through the list of images that were determined to go to DAMS.  To avoid duplicates,
                        we check DAMS for an already existing image before we choose to create a new one.
        RFeldman 3/2015
    */
    
    private void processList (CDIS cdis) {
         // See if we can find if this uan already exists in TMS
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            if (sqlTypeArr[0].equals("checkForExistingDAMSMedia")) {   
                sql = key;     
            }      
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String cisID : renditionsForDAMS.keySet()) {
                
                logger.log(Level.FINEST, "Processing for cisID: " + cisID);
                
                CDISMap cdisMap = new CDISMap();
                CDISActivityLog cdisActivity = new CDISActivityLog();
                
                String cisFileName = renditionsForDAMS.get(cisID);
                                
                // Now that we have the cisID, Add the media to the CDIS_MAP table
                boolean mapEntryCreated = cdisMap.createRecord(cdis, cisID, cisFileName);
                    
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
                
                String errorCode = null;
                
                
                try {
                       
                    errorCode = null;
                    sql = sql.replaceAll("\\?fileName\\?", cisFileName);
                
                    logger.log(Level.FINEST, "SQL: {0}", sql);
                    
                    boolean sentForIngest = false;
                     
                    stmt = damsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
                    
                    MediaFile mediaFile = new MediaFile();
                    
                    if (rs != null && rs.next()) {
                        //Find the image on the media drive
                        sentForIngest = mediaFile.sendToIngest(cdis, cisFileName, cisID);   
                    }
                    else {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMap.getCdisMapId(), "DUP", "Media Already exists: Media does not need to be created", damsConn);
                        continue;
                    }
                    
                    // If we have no error condition, mark status in activity table, else flag as error
                    if (! sentForIngest) {
                        errorCode = mediaFile.errorCode;
                        throw new Exception();
                    } else {
                        //Log into the activity table
                        activityLogged = cdisActivity.insertActivity(damsConn, cdisMap.getCdisMapId(), "SW");
                        if (!activityLogged) {
                            logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                            continue;
                        }
                    }

                } catch (Exception e) {
                    if (errorCode == null) {
                        errorCode = "PLE"; //Set error code to ProcessList error
                    }
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap.getCdisMapId(), errorCode, "File Copy Failure for FileName:" + cisFileName  + " " + e, damsConn);    
                    
                } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                }
            }
                        
        } 
        else {
            logger.log(Level.FINER, "ERROR: unable to check if CIS Media exists, supporting SQL not provided");
        }
    
    }

    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of CIS IDs that need to be integrated into DAMS
        RFeldman 3/2015
    */
    private void populateNewMediaList (CDIS cdis) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;

        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
              
            if (sqlTypeArr[0].equals("cisSelectList")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
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
                            return;
                    }
                   
                                                   
                    rs = stmt.executeQuery();
        
                    while (rs.next()) {           
                        addRenditionsForDAMS(rs.getString("cisID"), rs.getString("fileName"));
                    }   

            } catch (Exception e) {
                    logger.log(Level.FINER, "Error: obtaining CIS ID list ", e );
                    return;
                    
            } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
            
        }
        
        return;
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
            
            //Set the Master vatiable information
            String currentHotFolderMasterName = hotFolderBaseDir + "\\" + currentMainDirName + "\\MASTER";
            File hotFolderMasterDir = new File(currentHotFolderMasterName);
            logger.log(Level.FINER, "Current hotfolder set to : " + currentHotFolderMasterName);
            
            //Set the Workfile vatiable information
            String batchWorkFileDirName = hotFolderBaseDir + "\\" + currentMainDirName + "\\TEMP-XFER\\" + batchNumber;
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
                     
                    FileUtils.moveFileToDirectory(fileForDams, hotFolderMasterDir, false);
                     
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
                    errorLog.capture(cdisMap.getCdisMapId(), "MFH", "Move to Hotfolder MASTER Failure for FileName:" + cdisMap.getFileName()  + " " + e, damsConn );
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
                    "WHERE  a.cis_id = b.cis_id " +
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
    
    private Integer countHotFolderSubFiles(String subDir) {
    
        // Check to see if anything is in hotfolder.  If there is, we need to wait for hotfolder to clear or we can run the risk of 
        // having files being ingested that are partially there.
            
        Integer numFiles = 0;
        
        for(Iterator<String> iter = distinctHotFolders.iterator(); iter.hasNext();) {
            String hotFolderSubLocation = hotFolderBaseDir + "\\" + iter.next() + "\\" + subDir;
            
            File hotFolderSubDir = new File(hotFolderSubLocation);
        
            if(hotFolderSubDir.isDirectory()){ 
                numFiles += hotFolderSubDir.list().length;
                logger.log(Level.FINER, "Directory: " + hotFolderSubLocation + " Files: " + numFiles);
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
        populateNewMediaList (cdis);
        
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
        Integer numWorkFiles = countHotFolderSubFiles("TEMP-XFER\\" + cdis.getBatchNumber());
        if (! (numWorkFiles > 0)) {
            logger.log(Level.FINER, "No work Files detected to process, returning");
            return;
        }
        
        // Check to see if anything in any of the hotfolder directories.
        // We pause if there is a file in the hotfolder and wait for the hotfolder file to be ingested.
        
        logger.log(Level.FINER, "{0} Workfiles detected.  Need to check if ANY hotfiles exist from another process before continuing", numWorkFiles);
        while (countHotFolderSubFiles("MASTER") > 0) {
 
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
