/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.Database.CDISActivityLog;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

import edu.si.CDIS.CIS.Thumbnail;





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
            
            if (sqlTypeArr[0].equals("retrieveRenditionIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                mapIdsToSync.add(rs.getInt("cdis_map_id"));
                logger.log(Level.FINER,"Adding CIS renditionID for CISThumbnailSync update: " + rs.getInt("RenditionID") );
            }
            
        } catch (Exception e) {
            e.printStackTrace();
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
            
            Thumbnail thumbnail = new Thumbnail();
            boolean blobUpdated = thumbnail.generate (mapId);
             
            if (blobUpdated) {
                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(mapId);
                cdisActivity.setCdisStatusCd("CTS");    
                cdisActivity.insertActivity();
                
            }
        }
        
    }
    
}
