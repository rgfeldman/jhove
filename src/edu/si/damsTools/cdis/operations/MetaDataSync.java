/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import com.google.common.collect.HashBasedTable;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.operations.metadataSync.CisSqlCommand;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.operations.metadataSync.DamsTblColSpecs;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.DbUtils;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;


/**
 *
 * @author rfeldman
 */
public class MetaDataSync extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final Set<CdisMap> cdisMapList; //unique list of CDIS Map records that require metadata sync
    private final ArrayList<CisSqlCommand> cisSqlCommand;
    private final Set<DamsTblColSpecs> damsTblSpecs;  //we want to make sure to avoid duplicate entries here too.
     
    public MetaDataSync() {
        cdisMapList = new HashSet<>();
        cisSqlCommand = new ArrayList<>();
        damsTblSpecs = new HashSet<>();
    }
     
    public void invoke() {
        populateNeverSyncedMapIds();
          
        populateCisUpdatedMapIds();
          
        if (cdisMapList.isEmpty()) {
            logger.log(Level.FINEST,"No Rows found to sync");
            return;
        }
        
        populateCisSqlList();
        if (cisSqlCommand.isEmpty()) {
            logger.log(Level.FINEST, "Error: metadataMap queries not found in xml file");
            return;
        }
        
        populateDamsTblSpecs();
        if (damsTblSpecs.isEmpty()) {
            logger.log(Level.FINEST, "Error: unable to obtain statistics for DAMS tables");
            return;
        }
        
        
        processCdisMapListToSync();
                  
    }
    
    // Method: populateDamsTblColList()
    // Purpose: Populates the damsTableSpec list
    private boolean populateDamsTblSpecs() {
       for (CisSqlCommand sqlCommand : cisSqlCommand) {
           DamsTblColSpecs damsTblSpec = new DamsTblColSpecs(sqlCommand.getTableName());
           damsTblSpec.populateColumnWidthArray();
           damsTblSpecs.add(damsTblSpec);
       }
       return true;
    }
    
    
    /*private boolean buildCisQueryPopulateResults(CdisMap cdisMap) {
        
        
        //Loop through all of the queries for the current operation type
        for (String sql : this.metaDataMapQueries.keySet()) {
          
            
            if (sql.contains("?MEDIA_ID?")) {
                sql = sql.replace("?MEDIA_ID?", String.valueOf(cdisMap.getCisUniqueMediaId()));
            }
            if (sql.contains("?UOI_ID?")) {
                sql = sql.replace("?UOI_ID?", String.valueOf(cdisMap.getDamsUoiid()));
            }
            if (sql.contains("?OBJECT_ID?")) {
                CdisObjectMap objectMap = new CdisObjectMap();
                objectMap.setCdisMapId(cdisMap.getCdisMapId());
                objectMap.populateCisUniqueObjectIdforCdisId();
           
                sql = sql.replace("?OBJECT_ID?", String.valueOf(objectMap.getCisUniqueObjectId()));
            }
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            setSourceDb();
            try (PreparedStatement stmt = sourceDb.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

                while (rs.next()) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    
                    for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                        
                        String columnName = rsmd.getColumnLabel(i).toUpperCase();
                        String resultVal = rs.getString(i);
                        
                        logger.log(Level.ALL, "TABL: " + destTableName + " COLM: " + columnName + " VAL: " + resultVal);
                            
                        if (resultVal != null) {
                            //scrub the string to get rid of special characters that may not display properly in DAMS
                            resultVal = scrubString(resultVal);
                            
                            if (! columnName.equals("CDIS_TRANSLATE_IDS_SIZES") ) {                               
                                //Truncate the string if the length of the string exceeds the DAMS column width
                                if (resultVal.length() > columnLengthHashTable.get(destTableName, columnName)) {
                                    resultVal = resultVal.substring(0,columnLengthHashTable.get(destTableName, columnName));
                                }
                            }
                        }
                    
                        switch (operationType) {
                            case "U":
                                if (columnName.equals("CDIS_TRANSLATE_IDS_SIZES") ) {
                                    String internalSize;
                                    String externalSize;
                                            
                                    externalSize = calculateMaxIdsSize(resultVal);
                                    if (externalSize != null) {
                                        internalSize = "3000";
                                    }
                                    else {
                                        internalSize = calculateMaxIdsSize(resultVal, "INTERNAL");
                                        externalSize = calculateMaxIdsSize(resultVal, "EXTERNAL");
                                    }
                                    
                                    populateUpdateRowForDams(destTableName, "INTERNAL_IDS_SIZE", internalSize, appendDelimter);
                                    populateUpdateRowForDams(destTableName, "MAX_IDS_SIZE", externalSize, appendDelimter);
                                }
                                else {
                                    populateUpdateRowForDams(destTableName, columnName, resultVal, appendDelimter);
                                }
                                break;
                            case "DI":
                                deleteRows.add(destTableName);
                                if (resultVal != null) {
                                    insertRowForDams.put(destTableName, columnName, resultVal);
                                }
                                break;   
                            default:
                                //Error, unable to determine sync type
                                logger.log(Level.SEVERE, "Error: Unable to Determine sync type");
                                return false;
                        }
                        
                    }   
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain info for metadata sync", e);
                return false;
            }   
        }
      
        return true;
    }
    
    */
    
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
        Method :        populateMetadataSqlList
        Description:    populates object to hold Queries for all DAMS tables involved in metadata sync
    */
    private void populateCisSqlList() {
        
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            if (! xmlInfo.getAttributeData("type").equals("metadataMap")) {    
                continue;
            }
   
            if (xmlInfo.getDataValue() != null) {
                logger.log(Level.FINEST, "SQL: {0}", xmlInfo.getDataValue());  
                CisSqlCommand metadataSqlCommand = new CisSqlCommand();
                metadataSqlCommand.setValuesFromXml(xmlInfo);
                cisSqlCommand.add(metadataSqlCommand);
            }
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
    
    private void processCdisMapListToSync() {
        // for each UOI_ID that was identified for sync
        for (Iterator<CdisMap> iter = cdisMapList.iterator(); iter.hasNext();) {
            
            //commit with each iteration
            try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            ArrayList<DamsRecord> damsRecordList = new ArrayList<>();
            CdisMap cdisMap = new CdisMap();
            
            //Get the DamsRecord for this cdisMapId, and populate the required values
            DamsRecord damsRecord = new DamsRecord();
            damsRecord.setUoiId(cdisMap.getDamsUoiid());
            damsRecord.setBasicData();
            
            //Replace the values based on the damsRecord
            for (CisSqlCommand sqlCmd : cisSqlCommand) {
                String sql = sqlCmd.getSqlQuery();
                sql = damsRecord.replaceSqlVars(sql);
                
                if (sql.contains("?MEDIA_ID?")) {
                    sql = sql.replace("?MEDIA_ID?", String.valueOf(cdisMap.getCisUniqueMediaId()));
                }
                if (sql.contains("?OBJECT_ID?")) {
                    CdisObjectMap objectMap = new CdisObjectMap();
                    objectMap.setCdisMapId(cdisMap.getCdisMapId());
                    objectMap.populateCisUniqueObjectIdforCdisId();
           
                    sql = sql.replace("?OBJECT_ID?", String.valueOf(objectMap.getCisUniqueObjectId()));
                }
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
            
             processDamsRecord(damsRecord, cdisMap);
            
        }
    }
    
    private void processDamsRecord(DamsRecord dRec, CdisMap cdisMap) {
        
        //Add the related parent/children records to the list
            if (DamsTools.getProperty("syncParentChild") != null  && DamsTools.getProperty("syncParentChild").equals("true") ) {
                
                ArrayList<DamsRecord> relatedRecordList = new ArrayList<>();
                 //Get the related damsRcords (Parent/children)
             //   damsRecordList = damsRecord.returnRelatedDamsRecords ();
            }
            
             //Add the current damsReocord to the list
           // damsRecordList.add(damsRecord);
            
        //Translate the values 
        // execute the SQL statment to obtain the metadata and populate variables. The key value is the CDIS MAP ID
        //boolean dataMappedFromCIS = buildCisQueryPopulateResults(cdisMap);
        //if (! dataMappedFromCIS) {
            //    ErrorLog errorLog = new ErrorLog ();
            //    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
            //    noErrorFound = false;
            //    continue; 
            //}
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
