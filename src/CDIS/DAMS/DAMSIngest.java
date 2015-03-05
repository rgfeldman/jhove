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
import java.util.HashMap;
import java.util.logging.Level;
import com.jamesmurty.utils.XMLBuilder;

/**
 *
 * @author rfeldman
 */
public class DAMSIngest {
    
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection tmsConn;
    
    HashMap <String,String> renditionsForDAMS; 
    
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
            if (sqlTypeArr[0].equals("TMSSelectList")) {   
                sql = key;    
              
            }
            
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String RenditionID : renditionsForDAMS.keySet()) {
                
                String tmsFileName = renditionsForDAMS.get(RenditionID);
                    
                sql = sql.replaceAll("\\?fileName\\?", tmsFileName);
                
                logger.log(Level.FINEST, "SQL: {0}", sql);
                
                try {
                    stmt = tmsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
                       
                    if ( rs.next()) {   
                            
                            //build XML file
                        
                            //If the rendition is not in dams:
                            //Find the image on the media drive
                            
                            //copy the image to the hotfolder in DAMS
                        
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
    
     public void ingest (CDIS cdis_new, StatisticsReport statReport) {
         
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
        
        logger.log(Level.FINER, "In redesigned Ingest to Collections area");
        
        this.renditionsForDAMS = new HashMap<String, String>();
        
        // Populate the header for the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "IngestToDAMS");
        
        //Get the records from TMS that may need to go to DAMS
        populateRenditionsFromTMS (cdis_new);
        
        // check if the renditions are in dams
        checkDAMSForRendition(cdis_new);
                

     }
}
