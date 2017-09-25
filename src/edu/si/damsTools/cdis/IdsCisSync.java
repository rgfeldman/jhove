/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;

import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdis.cis.tms.MediaRecord;
import edu.si.damsTools.cdis.cis.aaa.CollectionRecord;
import edu.si.damsTools.cdis.database.CDISActivityLog;
import edu.si.damsTools.utilities.XmlSqlConfig;


import java.util.logging.Level;
import java.util.logging.Logger;

import edu.si.damsTools.DamsTools;


// This is the main entrypoint for syncing the image file and image file path in TMS
public class IdsCisSync extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
  
    private ArrayList<Integer> mapIdsToSync;

    XmlSqlConfig xml;
            
    public IdsCisSync() {
        xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        xml.setProjectCd(DamsTools.getProjectCd());
    }
    
    public void invoke() {
    	
        mapIdsToSync = new ArrayList<>();

        //Get list of renditions to sync
        boolean receivedList = getNeverSyncedImagePath();
        
        if (receivedList) {
            //loop through the list, and update the pathname
            processListFromXmlConfig ();
        }
        
        try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
    }
    
    // Get list of images that require sync file path to be updated
    private boolean getNeverSyncedImagePath () {
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getMapIds"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < DamsTools.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }    
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                     mapIdsToSync.add(rs.getInt("CDIS_MAP_ID"));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
                return false;
            }
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
                 
                switch (DamsTools.getProperty("cis")) {
                    case "tms" :
                        MediaRecord mediaRecord = new MediaRecord();
                        pathUpdated = mediaRecord.redirectPath(cdisMap);
                        break;
                        
                    case "aaa" :            
                        CollectionRecord collectionRecord = new CollectionRecord();      
                        pathUpdated = collectionRecord.pointToUans(cdisMap);
                        break;
                    
                    default :
                        logger.log(Level.FINER, "Error: Invalid CIS Type indicated in config file");  
                }
                
                if (pathUpdated) {
                    CDISActivityLog cdisActivity = new CDISActivityLog(); 
                    cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                    cdisActivity.setCdisStatusCd("CPS");    
                    boolean activityLogged = cdisActivity.insertActivity();
                    if (!activityLogged) {
                        logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
                    }
                }
                
                try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
            } catch (Exception e) {
                    logger.log(Level.FINER, "Error in IDS Sync loop", e);
            }
        }
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
}
