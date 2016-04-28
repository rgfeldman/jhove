/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;

import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.CIS.TMS.MediaPath;


import java.util.logging.Level;
import java.util.logging.Logger;

// This is the main entrypoint for syncing the image file and image file path in TMS
public class IdsCisSync {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
  
    private ArrayList<Integer> mapIdsToSync;
        
    public void sync() {
    	
        mapIdsToSync = new ArrayList<>();

        //Get list of renditions to sync
        boolean receivedList = getNeverSyncedImagePath();
        
        if (receivedList) {
            //loop through the list, and update the pathname
            processListFromXmlConfig ();
        }
    }
    
    // Get list of images that require sync file path to be updated
    private boolean getNeverSyncedImagePath () {
        String sqlTypeArr[] = null;
        String sql = null; 
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
              
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getMapIds")) {   
                sql = key;    
            }
        }      
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet  rs = pStmt.executeQuery() ) {
            
            logger.log(Level.FINEST,"SQL! " + sql); 
                 
            while (rs.next()) {
                logger.log(Level.ALL, "Adding to list to sync: " + rs.getInt(1));
                mapIdsToSync.add(rs.getInt("CDIS_MAP_ID"));
            }

        } catch (Exception e) {
            logger.log(Level.FINEST, "Error obtaining list to sync mediaPath and Name ", e);
            return false;
        } 
        return true;
    }
    
    
    // That the list of renditions by CDIS_ID and loop through them one at a time, and perform synchronizations
    private void processListFromXmlConfig() {

        Iterator mapIds = mapIdsToSync.iterator();
        
        while (mapIds.hasNext()) {
            try {
                // Reassign object with each loop
                CDISMap cdisMap = new CDISMap();
                   
                // set the MapId in the CDIS_MAP object
                cdisMap.setCdisMapId( (Integer) mapIds.next());
                
                // Obtain other values needed in CDIS_MAP object
                cdisMap.populateMapInfo();
                 
                MediaPath mediaPath = new MediaPath();
                mediaPath.redirectCisMediaPath(cdisMap);
                
                try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
            } catch (Exception e) {
                    e.printStackTrace();
            }
        }
    }
}
