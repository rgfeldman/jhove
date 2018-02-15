/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import com.google.common.collect.Table;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.operations.metadataSync.CisSqlCommand;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.operations.metadataSync.DamsTblColSpecs;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlQueryData;
import edu.si.damsTools.utilities.DbUtils;
import edu.si.damsTools.utilities.StringUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rfeldman
 */
public class MetaDataSync extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final Set<CdisMap> cdisMapList; //unique list of CDIS Map records that require metadata sync
    private final ArrayList<CisSqlCommand> cisSqlCommand;
    private final Set<DamsTblColSpecs> damsTblSpecs;  //we want to make sure to avoid duplicate entries here too.
    
    private ArrayList<String> deletesForDamsRecord;
    private HashMap<String, String> updatesForDamsRecord; //DAMS Table Name and the actual sql to run 
    private HashMap<String, ArrayList<String>> insertsByDamsRecord; 
    
     
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
        
        //Add the tablename to a list to make sure we dont query the database for the same table twice
        ArrayList <String> tableRecorded = new ArrayList <>();
        
        for (CisSqlCommand sqlCommand : cisSqlCommand) {
           
            //If we dont have the specs for this table yet, add the specs to the list
            if (! tableRecorded.contains(sqlCommand.getTableName() ) ) {
                //Create the specs data record and populate the values from the database
                DamsTblColSpecs damsTblSpec = new DamsTblColSpecs(sqlCommand.getTableName());           
                damsTblSpec.populateColumnWidthArray();
                damsTblSpecs.add(damsTblSpec);
                
                //Add the tablename to the tablelist 
                tableRecorded.add(sqlCommand.getTableName());
            }
       }
       return true;
    }
    
    
    private HashMap<String,String> returnCisQueryResults(String dbName, String sql) {
        
        HashMap<String,String> columnValHash = new HashMap<>();
        
        try (PreparedStatement stmt = DbUtils.returnDbConnFromString(dbName).prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    String columnName = rsmd.getColumnLabel(i).toUpperCase();
                    String resultVal = rs.getString(i);

                    columnValHash.put(columnName, resultVal);
                }
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain info for metadata sync", e);
            return null;
        }      
        return columnValHash;
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
                CisSqlCommand cisSqlCmd = new CisSqlCommand();
                cisSqlCmd.setValuesFromXml(xmlInfo);
                cisSqlCommand.add(cisSqlCmd);
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
            CdisMap cdisMap = iter.next();
            
            //commit with each iteration
            try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }

            //Get the DamsRecord for this cdisMapId, and populate the required values
            DamsRecord damsRecord = new DamsRecord();
            damsRecord.setUoiId(cdisMap.getDamsUoiid());
            damsRecord.setBasicData();
            
            deletesForDamsRecord = new ArrayList<>();
            insertsByDamsRecord = new HashMap<>();
            updatesForDamsRecord =  new HashMap<>();
        
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
                
                //Get a Hashmap containing all the columns and the results for the current query
                HashMap<String,String> columnValHash = new HashMap<>();
                columnValHash = returnCisQueryResults(sqlCmd.getDbName(), sql);   
                
                //Scrub the results of the hash and truncate if necessary
                columnValHash = cleanQueryResultsForDb(columnValHash, sqlCmd);
                
                // ALSO need to pass in the delete, insert and update structures
                createQueryLists(damsRecord, columnValHash, sqlCmd); 
            
            }
            
            metadataSyncRecords(cdisMap, damsRecord);
            
            //This is not supported at current time
           // if (DamsTools.getProperty("syncParentChild") != null  && DamsTools.getProperty("syncParentChild").equals("true") ) {

                //ArrayList<DamsRecord> dRecList = new ArrayList<>();
                
            //    dRecList = damsRecord.returnRelatedDamsRecords();
            
            //    for (DamsRecord dRecord : dRecList) {
            //        metadataSyncRecords(cdisMap, damsRecord);
            //    }
           // }

        }
    }
    
    private boolean metadataSyncRecords(CdisMap cdisMap, DamsRecord dRec) {
        //Perform any deletes that need to be run on the current DAMSID
        for (String sqlToDelete :deletesForDamsRecord) {
            updateDamsData(sqlToDelete);
        }
            
        //Perform any Inserts that need to be run on the current DAMSID
        for (String tableName : insertsByDamsRecord.keySet()) {
            ///NEED TO DO DB INSERTS!
            ArrayList<String> sqlToInsert = new ArrayList<String>();
            
            sqlToInsert = insertsByDamsRecord.get(tableName);
            for (String sqlStmnt :sqlToInsert  ) {
                int insertCount = updateDamsData(sqlStmnt);
                
                if (insertCount != 1) {
                    ErrorLog errorLog = new ErrorLog ();
                    //Get CDISMAPID by uoiId
                    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to insert DAMS metadata " + dRec.getUois().getUoiid() );    
                    return false;    
                }
            }   
        }      
        
        //Perform any updates that need to be run on the current DAMSID
        for (String tableName : updatesForDamsRecord.keySet()) {
            String sqlToUpdate = updatesForDamsRecord.get(tableName);
            sqlToUpdate = sqlToUpdate + " WHERE uoi_id  = '" +  cdisMap.getDamsUoiid() + "'";
            int updateCount = updateDamsData(sqlToUpdate);
                
            // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
            if (updateCount != 1) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " +  dRec.getUois().getUoiid() ); 
                return false;
            } 
        }
    
        if (DamsTools.getProperty("overrideUpdtDt") != null) {
             dRec.getUois().setMetadataStateDt(DamsTools.getProperty("overrideUpdtDt"));
        }
        else {
            dRec.getUois().setMetadataStateDt("SYSDATE");
        }
        
        int updateCount = dRec.getUois().updateMetaDataStateDate();
        if (updateCount != 1) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMD", "Error, unable to update uois table with new metadata_state_dt " + dRec.getUois().getUoiid());   
            return false; 
        }
        
        return true;
    }
    
    private HashMap<String,String>  cleanQueryResultsForDb(HashMap<String,String> columnValHash, CisSqlCommand cisSqlCmd) {
        
        HashMap<String,String> newHash = new HashMap<>();
        
        for (String column : columnValHash.keySet()) {
            String columnVal = columnValHash.get(column);
            
            //Handle nulls
            if (columnVal == null) {
                newHash.put(column, "");
                continue;
            } 
            
            //Replace special chars and quotes 
            columnVal = StringUtils.scrubString(columnVal);
            
            if (! column.equals("CDIS_TRANSLATE_IDS_SIZES") ) { 
                if (newHash.containsKey(column)) {
                    if (cisSqlCmd.getAppendDelimiter() != null ) {
                        //This column is already in map, append two results together
                        columnVal = newHash.get(column) + cisSqlCmd.getAppendDelimiter() + columnVal;
                    }
                    else {
                        logger.log(Level.ALL, "Warning: Select statement expected to return single row, returned multiple rows. populating with only one value");
                    }
                }
               
                //truncate the end of the value based on the column length of the DAMS table
                for (DamsTblColSpecs tblSpec : damsTblSpecs) {
                    if (tblSpec.getTableName().equals(cisSqlCmd.getTableName())) {
                        
                        if (columnVal.length() > tblSpec.getColumnLengthForColumnName(column))  {
                            columnVal = columnVal.substring(0,tblSpec.getColumnLengthForColumnName(column));
                        }
                    }  
                }    
                
                newHash.put(column, columnVal);
            }
            else {
                String internalSize;
                String externalSize;
                                            
                externalSize = calculateMaxIdsSize(columnVal);
                if (externalSize != null) {
                    internalSize = "3000";
                }
                else {
                    internalSize = calculateMaxIdsSize(columnVal, "INTERNAL");
                    externalSize = calculateMaxIdsSize(columnVal, "EXTERNAL");
                }
                newHash.put("INTERNAL_IDS_SIZE", internalSize);
                newHash.put("MAX_IDS_SIZE", externalSize);
            }
            logger.log(Level.FINEST, "COL " + column + " VAL: " +  columnVal );  
    
        }
     
        return newHash;
    }
    
    private String calculateMaxIdsSize (String tmsRemarks, String idsType) {
        
        String idsSize = null;
        String defaultIdsSize = "3000";
        
        try {
            Pattern p = Pattern.compile("MAX "+ idsType +" IDS SIZE = (\\d+)");
            Matcher m = p.matcher(tmsRemarks);
        
            if (m.find()) {
                idsSize = m.group(1);
            }
        
            //Validate the values are numeric.  0 (original size) is valid for external IDS, but not internal IDS.
            if (idsType.equals("INTERNAL") ) {
                 if (! (Integer.parseInt(idsSize) > 0)) {
                      idsSize = defaultIdsSize;
                 }
             }
             else if (idsType.equals("EXTERNAL") ) {
                 if (! (Integer.parseInt(idsSize) >= 0)) {
                      idsSize = defaultIdsSize;
                 }
             }
        
            logger.log(Level.FINEST, "size: " + idsSize);

        } catch(Exception e) {
            logger.log(Level.ALL, "IDS size not an integer, setting to default for " + idsType);
            idsSize = defaultIdsSize;
        }       
            
        return idsSize;
    }
    
    private String calculateMaxIdsSize (String tmsRemarks) {
        
        String idsSize = null;
        String defaultIdsSize = "3000";
        
        try {
            Pattern p = Pattern.compile("MAX IDS SIZE = (\\d+)");
            Matcher m = p.matcher(tmsRemarks);
        
            if (m.find()) {
                idsSize = m.group(1);
            }
            else {
                //We were unable to find size parameters with the legacy method.
                return null;
            }
        
            if (! (Integer.parseInt(idsSize) >= 0)) {
                idsSize = defaultIdsSize;
            }
        
            logger.log(Level.FINEST, "size: " + idsSize);

        } catch(Exception e) {
            logger.log(Level.ALL, "IDS size not an integer, setting external to default for Legacy Method");
            idsSize = defaultIdsSize;
        }       
            
        return idsSize;
    }
    
    private boolean createQueryLists(DamsRecord dRec, HashMap<String,String> columnValueHash, CisSqlCommand sqlCmd) {
            
 
        
        for (String column : columnValueHash.keySet()) {
            
            String value = columnValueHash.get(column);
            
            if (sqlCmd.getOperationType().equals("DI")) {
                
                deletesForDamsRecord.add("DELETE FROM towner." + sqlCmd.getTableName() + " WHERE UOI_ID = '" + dRec.getUois().getUoiid() + "'");
            
                ArrayList<String> sqlVals = new ArrayList<String>();
            
                if (value.contains("^MULTILINE_LIST_SEP^")) {
                     // If the value contains "^MULTILINE_LIST_SEP^" then we need to break that result down and perform two or more insert statements 
                    Pattern delim = Pattern.compile("^MULTILINE_LIST_SEP^");
                
                    String valuesToInsert[] = value.split(delim.quote("^") );
                                                                 
                    //We can have multiple inserts for a single table, the first column of the map holds the tablename,
                    //the list contains the insert statements
                    for (int i =0; i < valuesToInsert.length; i++ ) {
                    
                        if ( ! (valuesToInsert[i].equals("MULTILINE_LIST_SEP") ) ) {
                            sqlVals.add ("INSERT INTO towner." + sqlCmd.getTableName() + 
                            " (UOI_ID, " + column + ") VALUES ('" + 
                            dRec.getUois().getUoiid() + "','" + valuesToInsert[i] + "')");
                        }
                    
                        insertsByDamsRecord.put(sqlCmd.getTableName(), sqlVals);   
                    }
                }
                else {
                    //We only have a single row to insert into the table specified.
                    sqlVals.add  ("INSERT INTO towner." + sqlCmd.getTableName() + 
                        " (UOI_ID, " + column + ") VALUES ('" + 
                        dRec.getUois().getUoiid() + "','" + value + "')");
                
                    insertsByDamsRecord.put(sqlCmd.getTableName(), sqlVals);               
                }    
            }
            else {
        
                //never update these special fields in parent/child sync
                //if (parentChildSync) {
                //    switch (columnName) {
                //        case "ADMIN_CONTENT_TYPE" :
                //        case "IS_RESTRICTED" :
                //        case "MANAGING_UNIT" :
                //       case "MAX_IDS_SIZE" :
                //        case "INTERNAL_IDS_SIZE" :
                //        case "PUBLIC_USE" :                  
                //        case "SEC_POLICY_ID" :
                //        case "SI_DEL_RESTS" :
                //            continue;
                //    }
                // }
      
                
                if (updatesForDamsRecord.isEmpty() || updatesForDamsRecord.get(sqlCmd.getTableName()) == null) {             
                    updatesForDamsRecord.put(sqlCmd.getTableName(), "UPDATE towner." + sqlCmd.getTableName() + " SET " + column + "= '" + value + "'");               
                }
                else {
                    updatesForDamsRecord.put(sqlCmd.getTableName(), updatesForDamsRecord.get(sqlCmd.getTableName()) + ", " + column + "= '" + value + "'");
                }
            }   
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
    
     /*  Method :        updateDamsData
        Arguments:      
        Description:    Updates the DAMS with the metadata changes 
        RFeldman 2/2015
    */
    private int updateDamsData(String sqlUpdate) {

        Integer recordsUpdated = 0;
        
        sqlUpdate = sqlUpdate.replaceAll("null", "");
                
        logger.log(Level.FINEST,"SQL TO UPDATE: " + sqlUpdate);
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sqlUpdate)) {
 
            recordsUpdated = pStmt.executeUpdate(sqlUpdate);
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating DAMS data", e);
            return -1;    
        } 
        
        return recordsUpdated;

    }
}
