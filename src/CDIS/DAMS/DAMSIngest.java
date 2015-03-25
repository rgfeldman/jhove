/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.DAMS;

import CDIS.CDIS;
import CDIS.CollectionsSystem.Database.TMSRendition;
import CDIS.CollectionsSystem.MediaRecord;
import CDIS.CollectionsSystem.Thumbnail;
import CDIS.DAMS.Database.SiAssetMetaData;
import CDIS.StatisticsReport;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import com.jamesmurty.utils.XMLBuilder;
import java.io.File;
import java.io.FileWriter;
import java.util.Properties;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author rfeldman
 */
public class DAMSIngest {
    
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection tmsConn;
    Integer numberMediaFilesToIngest;
    Integer numberXmlFilesToIngest;
    String workFolderDir;
    String damsDropOffLocation;
    
    LinkedHashMap <String,String> renditionsForDAMS; 
    
    private void addRenditionsForDAMS (String renditionID, String filename) {
        this.renditionsForDAMS.put(renditionID, filename); 
    }
    
    private String getMediaLocation (Integer RenditionId) {
        return "";
        
    }
    
    private void checkDAMSForRendition (CDIS cdis_new) {
         // See if we can find if this uan already exists in TMS
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis_new.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
            if (sqlTypeArr[0].equals("checkForExistingDAMSRendition")) {   
                sql = key;    
              
            }
            
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String renditionID : renditionsForDAMS.keySet()) {
                
                String tmsFileName = renditionsForDAMS.get(renditionID);
                    
                sql = sql.replaceAll("\\?fileName\\?", tmsFileName);
                
                logger.log(Level.FINEST, "SQL: {0}", sql);
                
                try {
                    stmt = tmsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
                       
                    if ( rs.next()) {   
                            
                            //build XML file
                            MetaXMLFile metaXmlFile = new MetaXMLFile();
                            metaXmlFile.contentCreate(); 
                            
                            // Create the metadata xml file into the work folder
                            boolean metaFileCreated = metaXmlFile.create(cdis_new, tmsFileName, metaXmlFile.xml);
                            
                            if (!metaFileCreated) {
                                logger.log(Level.FINE, "Error, metadata XML file not able to be created, obtaining next rendition");
                                continue;
                            }
                            
                            logger.log(Level.FINER, "MetaData XML file created successfully");
                                         
                            //Find the image on the media drive
                            MediaFile mediaFile = new MediaFile();
                            mediaFile.create(cdis_new, tmsFileName, Integer.parseInt(renditionID), this.tmsConn);
                        
                    }
                    else {
                        logger.log(Level.FINER, "Media Already exists: Media does not need to be created");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
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
    
    private void populateRenditionsFromTMS (CDIS cdis_new) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis_new.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
              
            if (sqlTypeArr[0].equals("TMSSelectList")) {   
                sql = key;      
            }   
        }
        
        if (sql != null) {           
        
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                    stmt = tmsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
        
                    if (rs.next()) {           
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
    
    private void moveFilesToDamsDropOff () {
                
        //establish vars to hold the dropoff locations for the media and the XML files
        File damsXMLDropOffDir =  new File(this.damsDropOffLocation + "MetaData");
        File damsMediaDropOffDir =  new File(this.damsDropOffLocation + "Master");
                
        
        File workFolderDir = new File(this.workFolderDir);
        File[] filesForDams = workFolderDir.listFiles();
        
        // For each file in work folder, move to the xml dropoff location, or the media dropoff location
        for(int i = 0; i < filesForDams.length; i++) {
            File fileForDams = filesForDams[i];
            try {
                if(fileForDams.getName().endsWith(".xml")) {
                    //move the XML file to the XML directory
                    FileUtils.moveFileToDirectory(fileForDams, damsXMLDropOffDir, false);
                    numberMediaFilesToIngest ++;
                }
                else {
                    //move the image to the image directory
                    FileUtils.moveFileToDirectory(fileForDams, damsMediaDropOffDir, false);
                    numberXmlFilesToIngest ++;
                }
            } catch (Exception e) {
                    e.printStackTrace();
            } 
        }
        
    }
    
    private void createReadyFile () {
        try {
            if (numberMediaFilesToIngest > 1) {
                //Create the ready.txt file and put in the media location
                File readyFile = new File (this.damsDropOffLocation + "ready.txt");
            
                readyFile.createNewFile();
            
            }
            } catch (Exception e) {
                    e.printStackTrace();
            }
    }
    
     public void ingest (CDIS cdis_new, StatisticsReport statReport) {
         
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
        this.workFolderDir = cdis_new.properties.getProperty("workFolder");
        this.damsDropOffLocation = cdis_new.properties.getProperty("damsDropOffLocation");
        
        logger.log(Level.FINER, "In redesigned Ingest to Collections area");
        
        this.renditionsForDAMS = new LinkedHashMap<String, String>();
        
        // Populate the header for the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "ingestToDAMS");
        
        //Get the records from TMS that may need to go to DAMS
        populateRenditionsFromTMS (cdis_new);
        
        // check if the renditions are in dams
        checkDAMSForRendition(cdis_new);
        
        // move the media file and XML file from the work folder to the DAMS hotfolder location
        moveFilesToDamsDropOff();
        
        // Create ready.txt file to indicate to the DAMS ingest process that there is a batch of files awaiting for ingest
        createReadyFile();
        
        // Add TMS-INGEST to the source_system_id in DAMS
        
     }
}
