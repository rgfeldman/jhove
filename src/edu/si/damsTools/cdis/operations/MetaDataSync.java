/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.operations.metadataSync.DamsTblColSpecs;
import edu.si.damsTools.cdis.operations.metadataSync.XmlCisSqlCommand;
import edu.si.damsTools.cdis.operations.metadataSync.MetadataColumnData;
import edu.si.damsTools.cdis.operations.metadataSync.damsModifications.ModificationsForDams;
import edu.si.damsTools.cdis.operations.metadataSync.damsModifications.Deletion;
import edu.si.damsTools.cdis.operations.metadataSync.damsModifications.Insertion;
import edu.si.damsTools.cdis.operations.metadataSync.damsModifications.Update;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlQueryData;
import edu.si.damsTools.utilities.DbUtils;
import edu.si.damsTools.utilities.StringUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
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
    private final ArrayList<XmlCisSqlCommand> cisSqlCommands;
    private final Set<DamsTblColSpecs> damsTblSpecs;  //we want to make sure to avoid duplicate entries here too.
    
    private ArrayList<Deletion> deletesForDamsRecord;
    private ArrayList<Insertion> insertsForDamsRecord; 
    private ArrayList<Update> updatesForDamsRecord; 
    
     
    public MetaDataSync() {
        cdisMapList = new HashSet<>();
        cisSqlCommands = new ArrayList<>();
        damsTblSpecs = new HashSet<>();
    }
     
    public void invoke() {
        //get list of CDIS reocrds that have never been synced
        populateNeverSyncedMapIds();
          
        //get list of CDIS records that need re-syncing
        populateCisUpdatedMapIds();
          
        if (cdisMapList.isEmpty()) {
            logger.log(Level.FINEST,"No Rows found to sync");
            return;
        }
        
        //get list of SQL commands to get the metadata from the CIS
        populateCisSqlList();
        if (cisSqlCommands.isEmpty()) {
            logger.log(Level.FINEST, "Error: metadataMap queries not found in xml file");
            return;
        }
        
        //get specifications of DAMS tables that we are inserting/updateing./deleting into as part of the metadata sync
        populateDamsTblSpecs();
        if (damsTblSpecs.isEmpty()) {
            logger.log(Level.FINEST, "Error: unable to obtain statistics for DAMS tables");
            return;
        }

        //perform the sync
        processCdisMapListToSync();
                  
    }
    
    // Method: populateDamsTblColList()
    // Purpose: Populates the damsTableSpec list
    private boolean populateDamsTblSpecs() {
        
        //Add the tablename to a list to make sure we dont query the database for the same table twice
        ArrayList <String> tableRecorded = new ArrayList <>();
        
        for (XmlCisSqlCommand sqlCommand : cisSqlCommands) {
           
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
        if (dbConn == null) {
            logger.log(Level.SEVERE, "Error: :qDatabase to run query from is not specified");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = dbConn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {
            ResultSetMetaData rsmd = rs.getMetaData();
            
            String columnName = rsmd.getColumnLabel(1).toLowerCase();
            
            while (rs.next()) {

                ArrayList<Integer> cdisMapIds = new ArrayList<>();
                    
                CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
                //The new code...all will end up with identifier types sooner or later
                cdisCisIdentifierMap.setCisIdentifierCd(columnName);
                cdisCisIdentifierMap.setCisIdentifierValue(rs.getString(1));
                cdisMapIds = cdisCisIdentifierMap.returnCdisMapIdsForCisCdValue();
                        
                for (Integer cdisMapId : cdisMapIds) {
                    CdisMap cdisMap = new CdisMap();
                    cdisMap.setCdisMapId(cdisMapId);
                    cdisMap.populateMapInfo();
                    cdisMapList.add(cdisMap);
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
        Description:    populates object to hold CIs command to get metadata
    */
    private void populateCisSqlList() {
        
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            if (! xmlInfo.getAttributeData("type").equals("metadataMap")) {    
                continue;
            }
   
            if (xmlInfo.getDataValue() != null) {
                logger.log(Level.FINEST, "SQL: {0}", xmlInfo.getDataValue());  
                XmlCisSqlCommand cisSqlCmd = new XmlCisSqlCommand();
                cisSqlCmd.setValuesFromXml(xmlInfo);
                cisSqlCommands.add(cisSqlCmd);
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
    
    //Method: processCdisMapListToSync
    //Purpose: Goes through the list of CdisMap records that require syncing, pull each one out, one at a time and process
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
            insertsForDamsRecord = new ArrayList<>();
            updatesForDamsRecord =  new ArrayList<>();
            
            //Replace the values based on the damsRecord
            for (XmlCisSqlCommand xmlSqlCmd : cisSqlCommands) {
                String sql = xmlSqlCmd.getSqlQuery();
                sql = damsRecord.replaceSqlVars(sql);
                sql = replaceCisVarsInString(sql, cdisMap);

                //Create hashmap containing column names and values for the current record
                //MetadataColumnData metadataColumnDate = new ArrayList();
                ArrayList<MetadataColumnData> metadataColumnDataArr = new ArrayList();
                
                //HashMap<String, String> damsColumnValue = new HashMap<>();
                metadataColumnDataArr = retrieveCisColumnValueData(xmlSqlCmd, sql, damsRecord);
                
                // populate the query lists (deletesForDamsRecord, insertsByDamsRecord and updatesForDamsRecord
                populateSyncCmds(damsRecord, metadataColumnDataArr, xmlSqlCmd); 
                
            }
            
            if (deletesForDamsRecord.isEmpty() && deletesForDamsRecord.isEmpty() && updatesForDamsRecord.isEmpty()) {
                logger.log(Level.FINEST, "Nothing to modify for this record in DAMS, no need to continue ");
                //get the next record
                continue;
            }
            
            //Perform the actual metadata sync
            boolean recordsSynced = metadataSyncRecords(cdisMap, damsRecord);
            if (recordsSynced) {
                CdisActivityLog activityLog = new CdisActivityLog();
                activityLog.setCdisMapId(cdisMap.getCdisMapId());
                activityLog.setCdisStatusCd("MDS-" + DamsTools.getProperty("cis").toUpperCase());
                activityLog.updateOrInsertActivityLog();
            }

        }
    }
    
    //*THIS SHOUKD BE MOVED TO CDISRECORD
    private String replaceCisVarsInString(String sql, CdisMap cdisMap) {

        if (sql.contains("?CISID")) {
            Pattern p = Pattern.compile("\\?CISID-([A-Z][A-Z][A-Z])\\?");
            Matcher m = p.matcher(sql);
            
            if (m.find()) {
                
                CdisCisIdentifierMap cdisCisIdentifier = new CdisCisIdentifierMap();
                cdisCisIdentifier.setCdisMapId(cdisMap.getCdisMapId());
                cdisCisIdentifier.setCisIdentifierCd(m.group(1).toLowerCase());
                cdisCisIdentifier.populateCisIdentifierValueForCdisMapIdType(); 
     
                sql = sql.replace("?CISID-" + m.group(1) + "?", cdisCisIdentifier.getCisIdentifierValue());            
            }      
        }
       
        logger.log(Level.FINEST, "FIXED SQL: {0}", sql);
        
        return sql;
        
    }
    
    
    
    //Method: populateCisQueryResults
    //Purpose: Populates the query results from the CIS into a structure that holds the dams column name and column name
    private ArrayList<MetadataColumnData> retrieveCisColumnValueData(XmlCisSqlCommand xmlSqlCmd, String sql, DamsRecord damsRecord ) {

        if (DamsTools.getProjectCd().equals("aspace")) {
            callGetDescriptiveData(damsRecord.getSiAssetMetadata().getEadRefId());
        }
        
        //HashMap<String, String> damsColumnValue = new HashMap<>();
        ArrayList<MetadataColumnData> metadataColumnDataArr = new ArrayList();

        try (PreparedStatement stmt = DbUtils.returnDbConnFromString(xmlSqlCmd.getDbName()).prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    
                    String columnNm = rsmd.getColumnLabel(i).toUpperCase();
                    String columnVal = rs.getString(i);
                    MetadataColumnData metadataColumnData;
                    
                    if (columnNm == null) {
                        metadataColumnData = new MetadataColumnData(columnNm,"");
                        metadataColumnDataArr.add(metadataColumnData);
                        continue;
                    } 
                    
                    switch  (columnNm) {
                        case "CDIS_TRANSLATE_IDS_SIZES" :
                            String internalSize;
                            String externalSize;
                                            
                            //First try old style comments where external size was only option
                            externalSize = calculateMaxIdsSize(columnVal);
                            if (externalSize != null) {
                                internalSize = "3000";
                            }
                            else {
                                //We did not find old style comments, try 'new style' comments
                                internalSize = calculateMaxIdsSize(columnVal, "INTERNAL");
                                externalSize = calculateMaxIdsSize(columnVal, "EXTERNAL");
                            }
                            
                            metadataColumnData = new MetadataColumnData("INTERNAL_IDS_SIZE",internalSize);
                            metadataColumnDataArr.add(metadataColumnData);
                            
                            metadataColumnData = new MetadataColumnData("MAX_IDS_SIZE",externalSize);
                            metadataColumnDataArr.add(metadataColumnData);
                            break;
                            
                        default :
                            //Replace special chars and quotes 
                            columnVal = StringUtils.scrubSpecialChars(columnVal);
                            
                            String existingDataValue = retValIfColumnAlreadyInSync(metadataColumnDataArr, columnNm);
                            
                            if (existingDataValue != null) {
                                if (xmlSqlCmd.getAppendDelimiter() != null ) {
                                    columnVal = returnAppendedColumnData(existingDataValue, columnVal, xmlSqlCmd.getAppendDelimiter());
                                }
                                else {
                                    logger.log(Level.ALL, "Error: Select statement expected to return single row, returned multiple rows.");
                                    throw new Exception("Error recorded");
                                }
                            }    
                               
                             //truncate the end of the value based on the column length of the DAMS table
                            for (DamsTblColSpecs tblSpec : damsTblSpecs) {
                                    if (tblSpec.getTableName().equals(xmlSqlCmd.getTableName())) {    
                                        columnVal = StringUtils.truncateByByteSize(columnVal, tblSpec.getColumnLengthForColumnName(columnNm));  
                                    }  
                            }    
                            
                            if (existingDataValue == null) {  
                                metadataColumnData = new MetadataColumnData(columnNm,columnVal);
                                metadataColumnDataArr.add(metadataColumnData);
                            }
                            else {
                                metadataColumnData = new MetadataColumnData(columnNm,columnVal);
                                metadataColumnDataArr.set(metadataColumnData.getColumnName().indexOf(columnNm), metadataColumnData);
                            }
                    }
                }
            }        
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain info for metadata sync", e);
            return null;
        }      
        return metadataColumnDataArr;
    }    
    
    private String retValIfColumnAlreadyInSync (ArrayList<MetadataColumnData> metadataColumnDataArr, String columnNm) {
        
        for (MetadataColumnData mcd : metadataColumnDataArr) {
            if (mcd.getColumnName().equals(columnNm)) {
                return mcd.getColumnValue();
            }
        }
        return null;
        
    }
    
    private String returnAppendedColumnData(String existingData, String dataToAppend, String delimiter) {
        return  existingData + delimiter + dataToAppend;   
    }
        
    //Method: metadataSyncRecords
    //Purpose: performs the actual metadata sync
    
    private boolean metadataSyncRecords(CdisMap cdisMap, DamsRecord dRec) {
        //Perform any deletes that need to be run on the current DAMSID
        for (ModificationsForDams modsToDelete :deletesForDamsRecord) {
            modsToDelete.updateDamsData();
        }
        
        for (Insertion modsToInsert : insertsForDamsRecord) {
            int insertCount = modsToInsert.updateDamsData();
            
            if (insertCount != 1) {
                ErrorLog errorLog = new ErrorLog ();
                //Get CDISMAPID by uoiId
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to insert DAMS metadata " + dRec.getUois().getUoiid() );    
                return false;    
            }
        }
        
        for (Update modsToUpdate : updatesForDamsRecord) {
            
            modsToUpdate.setSql(modsToUpdate.getSql() + " WHERE uoi_id  = '" +  cdisMap.getDamsUoiid() + "'" );
            int updateCount = modsToUpdate.updateDamsData();
            
            if (updateCount != 1) {
                ErrorLog errorLog = new ErrorLog ();
                //Get CDISMAPID by uoiId
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " + dRec.getUois().getUoiid() );    
                return false;    
            }   
        }
     
        dRec.getUois().setMetadataStateDt(DamsTools.getProperty("mdsUpdtDt"));
                
        int updateCount = dRec.getUois().updateMetaDataStateDate();
        if (updateCount != 1) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMD", "Error, unable to update uois table with new metadata_state_dt " + dRec.getUois().getUoiid());   
            return false; 
        }
        
        return true;
    }
    
    //Method: calculateMaxIdsSize
    //Purpose: calculates the MaxIdsSize (this is specific for TMS and shouls likely be moved)
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
    
    //Method: calculateMaxIdsSize
    //Purpose: calculates the MaxIdsSize (this is specific for TMS and shouls likely be moved)
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
    
    //Method: populateSyncCmds
    //Purpose: populates the delete, insert and update lists for metadata sync
    private boolean populateSyncCmds(DamsRecord dRec, ArrayList<MetadataColumnData> metadataColumnDataArr, XmlCisSqlCommand sqlCmd) {

          for(MetadataColumnData metadataColumnData : metadataColumnDataArr) {
            
            logger.log(Level.FINEST, "DEBUG Column name: " + metadataColumnData.getColumnName());
            logger.log(Level.FINEST, "DEBUG Column value: " + metadataColumnData.getColumnValue());
            
            //change single quotes to double quotes or we cannot perform insert correctly.
            // NOTE: We cannot use the scrubSpecialChars for this purpose, because this has impact on string length, and it is possible to truncate
            // one of the quotes and not the other with truncateByByteSize, so we need to do this AFTER the truncate.  This is common place for it
            String dataValue = StringUtils.doubleQuotes(metadataColumnData.getColumnValue());
            
            switch (sqlCmd.getOperationType()) {
                case "DI" :
                    //First add deletion to delete list
                    Deletion deletion = new Deletion(dRec.getUois().getUoiid(),sqlCmd.getTableName());   
                    deletion.populateSql(sqlCmd.getDelClause());
                    deletesForDamsRecord.add(deletion);

                    boolean existingRecordAppended = false;
                    for (Iterator<Insertion> it = insertsForDamsRecord.iterator(); it.hasNext();) {
                        Insertion insertion = it.next();
                         // see if the destination table is already accounted for, if so add to the existing insert statement
                         if (insertion.getTableName().equals(sqlCmd.getTableName())) {
                             it.remove();
                            insertion.appendToExistingSql(metadataColumnData.getColumnName(),  dataValue);
                            insertsForDamsRecord.add(insertion);
                            existingRecordAppended = true;
                            break;
                         }
                    }
                    
                    if (!existingRecordAppended) {  
                        //Now add insertion to insert list.
                        // It may be possible to add more than one insert into DAMS for a single CIS      
                        String valuesToInsert[] = dataValue.split(Pattern.quote("^MULTILINE_LIST_SEP^") );
            
                        for (int i =0; i < valuesToInsert.length; i++ ) {
                            Insertion insertion = new Insertion (dRec.getUois().getUoiid(), sqlCmd.getTableName());
                            insertion.populateSql(metadataColumnData.getColumnName(),  valuesToInsert[i]);
                            insertsForDamsRecord.add(insertion);
                        }                
                    }
                    break;
                
                case "U" :
                  
                    existingRecordAppended = false;
                    for (Iterator<Update> it = updatesForDamsRecord.iterator(); it.hasNext();) {   
                        Update update = it.next();
                        
                        // see if the destincation table is already accounted for, if so add it to the existing update statement
                        if (update.getTableName().equals(sqlCmd.getTableName())) {
                            //remove then update existing record, then re-add to array
                            it.remove();
                            update.appendToExistingSql(metadataColumnData.getColumnName(),  dataValue);
                            updatesForDamsRecord.add(update);
                            existingRecordAppended = true;
                            break;
                        }
                    }
                    
                    if (!existingRecordAppended) {
                        Update update = new Update(dRec.getUois().getUoiid(), sqlCmd.getTableName());
                        update.populateSql(metadataColumnData.getColumnName(),  dataValue );
                        updatesForDamsRecord.add(update);
                    }
                    break;
                
                default :
                    return false;
               
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
        reqProps.add("mdsUpdtDt");
        
        //add more required props here
        return reqProps;    
    }
    
    
    //Note...this needs to be moved to xml file. 
    private boolean callGetDescriptiveData (String eadRefId) {
       
        String sql = "CALL getDescriptiveData (\"" + eadRefId + "\")";
        
        logger.log(Level.FINEST,"SQL:" + sql );
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql)) {
            //This only needs to happen in one place.  It gets set every time here which is not needed.
            
            pStmt.executeQuery();
       
        } catch (Exception e) {
               logger.log(Level.FINER, "Error in getDescriptiveData", e );
               return false;
        } 
       
       return true; 
                 
     }
}

