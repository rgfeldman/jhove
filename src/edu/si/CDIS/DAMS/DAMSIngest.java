/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.CDISError;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.io.File;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author rfeldman
 */
public class DAMSIngest {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection cisConn;
    String batchWorkFolder;
    String damsHotFolder;
    String ingestListSource;
    
    LinkedHashMap <String,String> renditionsForDAMS; 
    
    private void addRenditionsForDAMS (String cisID, String filename) {
        this.renditionsForDAMS.put(cisID, filename); 
    }
    
    /*  Method :        checkDAMSForMedia
        Arguments:      
        Returns:      
        Description:    Goes through the list of RenditionIDs from TMS.  To avoid duplicates,
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
                
                CDISMap cdisMap = new CDISMap();
                String cisFileName = renditionsForDAMS.get(cisID);
                                
                // Now that we have the cisID, Add the media to the CDIS_MAP table
                boolean mapEntryCreated = cdisMap.createRecord(cdis, cisID, cisFileName);
                    
                if (!mapEntryCreated) {
                    logger.log(Level.FINER, "Could not create CDISMAP entry, retrieving next row");
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
                    
                    if (rs.next()) {
                        //Find the image on the media drive
                        sentForIngest = mediaFile.sendToIngest(cdis, cisFileName, cisID, this.ingestListSource);
                    }
                    else {
                        handleErrorMsg(cdisMap, "DUP", "Media Already exists: Media does not need to be created");
                        continue;
                    }
                    
                    // If we have no error condition, mark status in CDIS table, else flag as error
                    if (! sentForIngest) {
                        errorCode = mediaFile.errorCode;
                        throw new Exception();
                    }

                } catch (Exception e) {
                    if (errorCode == null) {
                        errorCode = "PLE"; //Set error code to ProcessList error
                    }
                    handleErrorMsg(cdisMap, errorCode, "File Copy Failure for FileName:" + cisFileName  + " " + e );
                    
                } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                }
            }
                        
        } 
        else {
            logger.log(Level.FINER, "ERROR: unable to check if TMS Media exists, supporting SQL not provided");
        }
    
    }
    
    private void handleErrorMsg (CDISMap cdisMap, String errorCode, String logMessage) {
        logger.log(Level.FINER, logMessage);
        
        CDISError cdisError = new CDISError();
        cdisMap.updateStatus(damsConn, 'E');
        cdisError.insertError(damsConn, "STDI", cdisMap.getCdisMapId(), errorCode);
    }
    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of TMS RenditionIDs that need to be integrated into DAMS
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
                    switch (this.ingestListSource) {
                        case "TMSDB" :
                            stmt = cisConn.prepareStatement(sql); 
                            break;
                        case "CDISDB" :
                            stmt = damsConn.prepareStatement(sql);
                            break;
                         default:     
                            logger.log(Level.SEVERE, "Fatal Error: Invalid ingest source {0}, exiting", this.ingestListSource );
                            return;
                    }
                                                   
                    rs = stmt.executeQuery();
        
                    while (rs.next()) {           
                        addRenditionsForDAMS(rs.getString("cisID"), rs.getString("fileName"));
                    }   

            } catch (Exception e) {
                    logger.log(Level.FINER, "Error: obtaining RendtionList ", e );
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
               
        //establish vars to hold the dropoff locations for the media files
        File damsMediaDropOffDir =  new File(this.damsHotFolder);
        
        File batchWorkFileDir = new File(batchWorkFolder);
        File[] filesForDams = batchWorkFileDir.listFiles();
        
        // For each file in work folder, move to the xml dropoff location, or the media dropoff location
        for(int i = 0; i < filesForDams.length; i++) {
            
            //Get MapID for logging and error capturing
            CDISMap cdisMap = new CDISMap();
            cdisMap.setBatchNumber(batchNumber);
            cdisMap.setFileName(filesForDams[i].getName()); 
            cdisMap.populateIDForFileBatch(this.damsConn);
            
            try { 
                               
                //skip the file if named Thumbs.db
                if (cdisMap.getFileName().equals("Thumbs.db")) {
                    continue;
                }
                    
            
                File fileForDams = filesForDams[i];
                        
               //move the image to the image directory if it was part of the batch
               logger.log(Level.FINER, "Moving image file to : " + this.damsHotFolder);
                
               FileUtils.moveFileToDirectory(fileForDams, damsMediaDropOffDir, false);
               cdisMap.updateStatus(this.damsConn, 'C');
               
            } catch (Exception e) {
                    logger.log(Level.FINER, "Error: Moving file to HotFolder ", e );
                    handleErrorMsg(cdisMap, "MFH", "Move to Hotfolder Failure for FileName:" + cdisMap.getFileName()  + " " + e );
            } 
        }
        
    }
    
   /*  Method :        createReadyFile
        Arguments:      
        Returns:      
        Description:    Creates empty file named 'ready.txt' in hot folder.
                        This file indicates for the DAMS to create images based on the files in the hotfolder location
        RFeldman 3/2015
    */
    private void createReadyFile () {
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = this.damsHotFolder + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
                    e.printStackTrace();
        }
    }
    
    /*  Method :        ingest
        Arguments:      
        Returns:      
        Description:    The main entrypoint or 'driver' for the ingestToDams operation Type
        RFeldman 3/2015
    */
     public void ingest (CDIS cdis) {
                                                                                       
        this.damsConn = cdis.damsConn;
        this.cisConn = cdis.cisConn;
        this.batchWorkFolder = cdis.properties.getProperty("workFolder") + "//" + cdis.getBatchNumber();
        this.damsHotFolder = cdis.properties.getProperty("hotFolderMaster");
        this.ingestListSource = cdis.properties.getProperty("ingestListSource");
  
        this.renditionsForDAMS = new LinkedHashMap<String, String>();
        
        // Get the list of new Media to add to DAMS
        populateNewMediaList (cdis);
        
        // check if the renditions are in dams...and process each one in list (move to workfolder
        processList(cdis);
        
        // Check to see if anything in the workfolder directory.
        // We only need to continue if there is a file there.
        File batchWorkFolderDir = new File(this.batchWorkFolder);
        if(batchWorkFolderDir.isDirectory()){ 
            if (! (batchWorkFolderDir.list().length>0)) {
                logger.log(Level.FINER, "No files found in workfolder. Exiting ingest Code");
                return;
            }
        } else {
            logger.log(Level.FINER, "No workfolder found. Exiting ingest Code");
            return;
        }
        
        // Check to see if anything is in hotfolder.  If there is, we need to wait for hotfolder to clear or we can run the risk of 
        // having files being ingested that are partially there.
        
        File hotFolderDir = new File(this.damsHotFolder);
        
        if(hotFolderDir.isDirectory()){ 
            while (hotFolderDir.list().length>0) {
 
		System.out.println("Directory is not empty.  Check back in 5 minutes");
                try {
                    Thread.sleep(300000);
                 } catch (Exception e) {
                    logger.log(Level.FINER, "Exception in sleep ", e);
                }
            }
        }
        else {
            logger.log(Level.FINER, "Error: Unable to find HotFolder, returning ");
            return;
        }
        
        //Move from workfolder to hotfolder
        moveFilesToHotFolder(cdis.getBatchNumber());
        
        // create the ready file
        createReadyFile();
         
     }
}
