/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.StatisticsReport;
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
    Integer numberMediaFilesToIngest;
    String workFolderDir;
    String damsHotFolder;
    Integer numberSuccessFiles;
    Integer numberFailFiles;
    
    LinkedHashMap <String,String> renditionsForDAMS; 
    
    private void addRenditionsForDAMS (String renditionID, String filename) {
        this.renditionsForDAMS.put(renditionID, filename); 
    }
    
    /*  Method :        checkDAMSForImage
        Arguments:      
        Returns:      
        Description:    Goes through the list of RenditionIDs from TMS.  To avoid duplicates,
                        we check DAMS for an already existing image before we choose to create a new one.
        RFeldman 3/2015
    */
    private void checkDAMSForImage (CDIS cdis, StatisticsReport statRpt) {
         // See if we can find if this uan already exists in TMS
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            if (sqlTypeArr[0].equals("checkForExistingDAMSImage")) {   
                sql = key;     
            }      
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String renditionID : renditionsForDAMS.keySet()) {
                
                String tmsFileName = renditionsForDAMS.get(renditionID);
                    
                sql = sql.replaceAll("\\?fileName\\?", tmsFileName);
                
                logger.log(Level.FINEST, "SQL: {0}", sql);
                
                boolean fileCreated = false;
                
                try {
                    stmt = damsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
                       
                    if ( rs.next()) {   
                                                                     
                            //Find the image on the media drive
                            MediaFile mediaFile = new MediaFile();
                            fileCreated = mediaFile.create(cdis, tmsFileName, Integer.parseInt(renditionID), this.cisConn);
                    }
                    else {
                        logger.log(Level.FINER, "Media Already exists: Media does not need to be created");
                    }
                    
                    if (! fileCreated) {
                        statRpt.writeUpdateStats("", tmsFileName, "ingestToDAMS", false);
                        numberFailFiles ++;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    statRpt.writeUpdateStats("", tmsFileName, "ingestToDAMS", false);
                    numberFailFiles ++;
                    
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
    
    /*  Method :        populateRenditionsFromTMS
        Arguments:      
        Returns:      
        Description:    Adds to the list of TMS RenditionIDs that need to be integrated into DAMS
        RFeldman 3/2015
    */
    private void populateRenditionsFromTMS (CDIS cdis) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
              
            if (sqlTypeArr[0].equals("TMSSelectList")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                    stmt = cisConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
        
                    while (rs.next()) {           
                        addRenditionsForDAMS(rs.getString("RenditionID"), rs.getString("Filename"));
                    }   

            } catch (Exception e) {
                    e.printStackTrace();
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
    private void moveFilesToHotFolder (StatisticsReport statRpt) {
                
        //establish vars to hold the dropoff locations for the media files
        File damsMediaDropOffDir =  new File(this.damsHotFolder);
        
        File workFolderDir = new File(this.workFolderDir);
        File[] filesForDams = workFolderDir.listFiles();
        
 
        
        // For each file in work folder, move to the xml dropoff location, or the media dropoff location
        for(int i = 0; i < filesForDams.length; i++) {
            
            //skip the file if named thumbs.db
            if (filesForDams[i].getName().equals("thumbs.db")) {
                // skip the file if it is called thumbs.db
                continue;
            }
                    
            File fileForDams = filesForDams[i];
            try {
                this.numberMediaFilesToIngest ++;
                //move the image to the image directory
                logger.log(Level.FINER, "Moving image file to : " + this.damsHotFolder);
                
                FileUtils.moveFileToDirectory(fileForDams, damsMediaDropOffDir, false);
                this.numberSuccessFiles ++;
                
                statRpt.writeUpdateStats("", fileForDams.getName(), "ingestToDAMS", true);
      
            } catch (Exception e) {
                    e.printStackTrace();
                    statRpt.writeUpdateStats("", fileForDams.getName(), "ingestToDAMS", false);
                    numberFailFiles ++;
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
            if (numberMediaFilesToIngest > 0) {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = this.damsHotFolder + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
            
            }
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
     public void ingest (CDIS cdis, StatisticsReport statReport) {
         
        this.damsConn = cdis.damsConn;
        this.cisConn = cdis.cisConn;
        this.workFolderDir = cdis.properties.getProperty("workFolder");
        this.damsHotFolder = cdis.properties.getProperty("hotFolderMaster");
        
        logger.log(Level.FINER, "In redesigned Ingest to CIS area");
        
        this.renditionsForDAMS = new LinkedHashMap<String, String>();
        
        // Count of records to ingest
        this.numberMediaFilesToIngest = 0;
        this.numberSuccessFiles = 0;
        this.numberFailFiles = 0;
        
        // Populate the header for the report file
        statReport.populateHeader(cdis.properties.getProperty("siUnit"), "ingestToDAMS");
        
        //Get the records from TMS that may need to go to DAMS
        populateRenditionsFromTMS (cdis);
        
        // check if the renditions are in dams
        checkDAMSForImage(cdis, statReport);
        
        // move the media file and XML file from the work folder to the DAMS hotfolder location
        moveFilesToHotFolder(statReport);
        
        statReport.populateStats (0, 0, this.numberSuccessFiles, this.numberFailFiles, "ingestToDAMS");
        
        // Create ready.txt file to indicate to the DAMS ingest process that there is a batch of files awaiting for ingest
        createReadyFile();
        
     }
}
