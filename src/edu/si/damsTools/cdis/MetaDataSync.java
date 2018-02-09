/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.dams.database.MetadataTransaction;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.utilities.DbUtils;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;


/**
 *
 * @author rfeldman
 */
public class MetaDataSync extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final Set<CdisMap> cdisMapList; //unique list of CDIS Map records that require metadata sync
    private final ArrayList<MetadataTransaction> mdTransList;
     
    public MetaDataSync() {
        cdisMapList = new HashSet<>();
        mdTransList = new ArrayList<>();
    }
     
    public void invoke() {
        populateNeverSyncedMapIds();
          
        populateCisUpdatedMapIds();
          
        if (cdisMapList.isEmpty()) {
            logger.log(Level.FINEST,"No Rows found to sync");
            return;
        }
        
        populateMetadataTransList();
                  
    }
    
    // Method: populateCisUpdatedMapIds()
    // Purpose: Populates the list of CdisMap records with records that have been updated in the CIS and require a re-sync
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
            
        try (PreparedStatement stmt = dbConn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {
            
            while (rs.next()) {
                   
                if (rs.getString(2) == null) {
                    //id type should not be null, but need to support the old code
                    CdisMap cdisMap = new CdisMap();
                    cdisMap.setCisUniqueMediaId(rs.getString(1));
                    boolean cisMediaIdObtained = cdisMap.populateIdFromCisMediaId();
                    
                    if (!cisMediaIdObtained) {
                        continue;
                    }
                    
                    cdisMap.setCdisMapId(rs.getInt(1));
                    cdisMap.populateMapInfo();
                    cdisMapList.add(cdisMap);
                }
                else {
                    CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
                    //The new code...all will end up with identifier types sooner or later
                    ArrayList<Integer> cdisMapIds = cdisCisIdentifierMap.returnCdisMapIdsForCisCdValue();
                        
                    for (Integer cdisMapId : cdisMapIds) {
                        CdisMap cdisMap = new CdisMap();
                        cdisMap.setCdisMapId(rs.getInt(1));
                        cdisMap.populateMapInfo();
                        cdisMapList.add(cdisMap);
                    }                   
                }
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error obtaining list to re-sync", e);
            return false;
        }    
       return true;
    }
    
    /*
        Method :        populateMetadataTransList
        Description:    populates object to hold Queries for all DAMS tables involved in metadata sync
    */
    private void populateMetadataTransList() {
        
        String sql = null;
        
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            if (! xmlInfo.getAttributeData("type").equals("metadataMap")) {    
                continue;
            }
   
            sql = xmlInfo.getDataValue();
            logger.log(Level.FINEST, "SQL: {0}", sql); 
 
            if (sql != null) {
               
                MetadataTransaction metadataTransaction = new MetadataTransaction();
                
                metadataTransaction.setSqlQuery(sql);
                metadataTransaction.setAppendDelimeter(xmlInfo.getAttributeData("multiResultDelim"));
                metadataTransaction.setTableName(xmlInfo.getAttributeData("destTableName"));
                metadataTransaction.setOperationType(xmlInfo.getAttributeData("operationType"));
                metadataTransaction.setDbName(xmlInfo.getAttributeData("dbConn"));
               
                mdTransList.add(metadataTransaction);
                   
            }
        }
        if (mdTransList == null) {
            logger.log(Level.FINEST, "Error: metadataMap queries not found in xml file");
            return;
        }

    }
    
        // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that have never been metadata synced and require syncing
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
    
    /* Member Function: returnRequiredProps
     * Purpose: Returns a list of required properties that must be set in the properties file in otder to run the cisUpate operation
     */  
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("metadataSyncXmlFile");
        
        //add more required props here
        return reqProps;    
    }
}
