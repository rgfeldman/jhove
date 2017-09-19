/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISRefIdMap;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.CDIS.DAMS.Database.SiAssetMetadata;
import edu.si.CDIS.CIS.ArchiveSpace.CDISUpdates;
import edu.si.Utils.XmlSqlConfig;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */

class CisUpdate {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList<Integer> mapIdsToSync;
    
    private boolean populateRefIdsToSync() {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        xml.setProjectCd(CDIS.getProjectCd());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("retrieveMapIds"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }    
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                     mapIdsToSync.add(rs.getInt(1));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
                return false;
            }
        }
    
        return true;
    }
    
    private void processRefList () {
        
        for (Integer mapId : this.mapIdsToSync) {
            try {
                CDISMap cdisMap = new CDISMap();
                
                cdisMap.setCdisMapId( mapId);
                boolean mapInfoPopulated = cdisMap.populateMapInfo();
                           
                boolean refIdUpdated = updateRefId(cdisMap);
                        
                if (!refIdUpdated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPCISR", "Error, RefIdUpdate failed");  
                    continue;
                }
                
                //Insert row in the activity_log as completed. COMMENTED OUT FOR NOW
                CDISActivityLog cdisActivity = new CDISActivityLog(); 
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("CPD"); 
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
                }
            
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            } catch (Exception e) {
                    logger.log(Level.FINER, "Error in Cis Update loop", e);  
            }
        }
        
    }
    
    private boolean updateRefId (CDISMap cdisMap) {
        
        //Get the RefId
        CDISRefIdMap cdisRefIdMap = new CDISRefIdMap();
        cdisRefIdMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisRefIdMap.populateRefIdFromMapId();
        
        //Check to see if this refId has been sent already
        int numRefIdsSent = countRefIdSent(cdisRefIdMap.getRefId());
        
        //If the RefID has not yet been sent, Update the CIS information
        if (numRefIdsSent == 0) {
            
            SiAssetMetadata siAsst = new SiAssetMetadata();
            siAsst.setUoiid(cdisMap.getDamsUoiid());
            siAsst.populateOwningUnitUniqueName();
            
            String holdingUnit = siAsst.getOwningUnitUniqueName().substring(0,siAsst.getOwningUnitUniqueName().indexOf("-"));
           
            CDISUpdates cdisUpdates = new CDISUpdates();
            
            cdisUpdates.setEadRefId(cdisRefIdMap.getRefId());
            cdisUpdates.setHoldingUnit(holdingUnit);

            if (cdisUpdates.getEadRefId() == null || cdisUpdates.getHoldingUnit() == null) {
                logger.log(Level.FINEST,"Error: Missing required information for ArchiveSpace");
                return false;
            }
            
            boolean recordCreated = cdisUpdates.createRecord();
            if (recordCreated != true) {
                return false;
            }        
        }
        
        return true;
    }
    
    private int countRefIdSent (String refId) {
        String sql =   "SELECT count(*) " +
                        "FROM   cdis_map map, " +
                        "       cdis_ref_id_map ref, " +
                        "       cdis_activity_log act " +
                        "WHERE ref.cdis_map_id = act.cdis_map_id " +
                        "AND   map.cdis_map_id = act.cdis_map_id " +
                        "AND   act.cdis_status_cd = 'CPD' " +
                        "AND   ref.ref_id = '" + refId + "'";
                        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getInt(1);
            }   
            else {
                // we need a map id, if we cant find one then raise error
                return 0;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Did not detect RefId Sent, will send new one ", e );
            return -1;
        }
         
    }
    
    public void updateCisFromDams() {
        
        mapIdsToSync = new ArrayList<>();
        
        boolean receivedList = populateRefIdsToSync();
        
        if (receivedList) {
            
            processRefList ();
        }
        
        try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }
}
