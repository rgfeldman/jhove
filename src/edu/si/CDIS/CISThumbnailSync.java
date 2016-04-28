/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.Database.CDISActivityLog;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.CDIS.Database.CDISMap;

import edu.si.CDIS.CIS.TMS.Thumbnail;


public class CISThumbnailSync {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList <Integer> mapIdsToSync;  
     
    /*  Method :        populateRenditionsToUpdate
        Arguments:      
        Description:    Populate a list of thumnails that need to be generated/updated 
        RFeldman 3/2015
    */
    private void populateIdsToUpdate () {
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("retrieveMapIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                mapIdsToSync.add(rs.getInt("cdis_map_id"));
                logger.log(Level.FINER,"Adding CIS renditionID for CISThumbnailSync update: " + rs.getInt("cdis_map_id") );
            }
            
        } catch (Exception e) {
           logger.log(Level.FINER,"Error, unable to get list of records to thumbnail sync: ", e );
        }    
        return;  
    }
    
    /*  Method :        sync
        Arguments:      
        Description:    CISThumbnailSync sync driver 
        RFeldman 3/2015
    */
    public void sync () {
        
        this.mapIdsToSync = new ArrayList <Integer>();
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateIdsToUpdate ();
        
        //create the thumbnail in TMS from those DAMS images 
        for (Integer mapId : mapIdsToSync) {
            CDISMap cdisMap = new CDISMap();
            cdisMap.setCdisMapId(mapId);
            cdisMap.populateMapInfo();
            
            Thumbnail thumbnail = new Thumbnail();
            boolean blobUpdated = thumbnail.generate (mapId);
            
            if (! blobUpdated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCIST", "ERROR: CIS Thumbnail Generation Failed"); 
                continue;
            }
            
            // we have successfully updated the Blob if we got to this point, mark it as success in the db
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(mapId);
            cdisActivity.setCdisStatusCd("CTS");    
            cdisActivity.insertActivity();
                
            try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
        }
        
        //commit anything not committed yet
        try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }
    
    
}
