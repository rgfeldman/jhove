/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.utilities.DbUtils;
import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class MetaDataSync extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<CdisMap> cdisMapList;
     
    public MetaDataSync() {
        cdisMapList = new ArrayList();
    }
     
    public void invoke() {
          boolean recordsToSync = populateNeverSyncedMapIds();
          
          recordsToSync = populateCisUpdatedMapIds();
    }
    
    private boolean populateCisUpdatedMapIds() {
        Connection dbConn = null;
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","getRecordsForResync");
            if (sql != null) {
                dbConn = DbUtils.returnDbConnFromString(xmlInfo.getAttributeData("dbConn"));  
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql getRecordsForResync not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
       /* try (PreparedStatement stmt = dbConn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                CdisMap cdisMap = new CdisMap();
                    
                if (DamsTools.getProperty("cis").equals("aspace")){
                    CdisCisIdentifierMap cdisCisGroupMap = new CdisCisIdentifierMap();
                    cdisCisGroupMap.setCisGroupValue(sql);
                    cdisCisGroupMap.setCisGroupValue(rs.getString(1));
                    cdisCisGroupMap.setCisGroupCd("ead");
                        
                    ArrayList<Integer> mapIdsForRefId = new ArrayList<>();;              
                    mapIdsForRefId =  cdisCisGroupMap.returnCdisMapIdsForCdValue();
                        
                    for (Integer mapId : mapIdsForRefId ) {
                        if (!cdisMapIdsToSync.contains(mapId)) {
                            cdisMapIdsToSync.add(mapId);
                        }
                    }       
                }
                else {
                    cdisMap.setCisUniqueMediaId(rs.getString(1));
                    boolean cisMediaIdObtained = cdisMap.populateIdFromCisMediaId();
                    
                    if (!cisMediaIdObtained) {
                        continue;
                    }
                        
                    //Only add to the list if it is not already in the list. It could be there from never synced record list
                    if (!cdisMapIdsToSync.contains(cdisMap.getCdisMapId())) {
                        cdisMapIdsToSync.add(cdisMap.getCdisMapId());
                    }
                } 
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error obtaining list to re-sync", e);
            return;
        }    */
       return true;
    }
      
     // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that require linking using the criteria in the xml file
    private boolean populateNeverSyncedMapIds() {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList() ) {
            sql = xmlInfo.getDataForAttribute("type","getNeverSyncedRecords");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {
                
            while (rs.next()) {

                logger.log(Level.ALL, "Adding to list to sync: " + rs.getInt(1));
                CdisMap cdisMap = new CdisMap();
                cdisMap.setCdisMapId(rs.getInt(1));
                cdisMap.populateMapInfo();
                cdisMapList.add(cdisMap);
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error obtaining list to sync", e);
            return false;
        }
        
        return true;
    }
     
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
}
