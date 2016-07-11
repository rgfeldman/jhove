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
import edu.si.CDIS.CIS.AAA.Database.TblDigitalResource;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.utilties.ErrorLog;


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
                
                //reset the indicator that shows if the path was reset successfully
                boolean pathUpdated = false;
                 
                switch (CDIS.getProperty("cisSourceDB")) {
                    case "TMS" :
                        MediaPath mediaPath = new MediaPath();
                        pathUpdated = mediaPath.redirectCisMediaPath(cdisMap);
                        
                    case "AAA" :
                        TblDigitalResource tblDigitalResource = new TblDigitalResource();
                        SiAssetMetaData siAsst = new SiAssetMetaData();

                        //Get the uan 
                        siAsst.setUoiid(cdisMap.getDamsUoiid());
                        siAsst.populateOwningUnitUniqueName();
                        
                        //assign the uan and digital resourceID
                        tblDigitalResource.setDamsUan(siAsst.getOwningUnitUniqueName());
                        tblDigitalResource.setDigitalResourceId(Integer.parseInt(cdisMap.getCisUniqueMediaId() ));
                        pathUpdated = tblDigitalResource.updateDamsUAN();
                        
                    default :
                        logger.log(Level.FINER, "Error: Invalid CIS Type indicated in config file");  
                }
                
                if (! pathUpdated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPCISU", "Error: unable to update media Path");
                }
                
                CDISActivityLog cdisActivity = new CDISActivityLog(); 
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("CPS");    
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
                }
                
                try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
            } catch (Exception e) {
                    e.printStackTrace();
            }
        }
    }
}
