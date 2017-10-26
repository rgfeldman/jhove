
package edu.si.damsTools.cdis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.sql.Connection;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.database.CdisCisGroupMap;
import edu.si.damsTools.cdis.cis.archiveSpace.CDISUpdates;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.dams.MediaRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlQueryData;


public class MetaDataSync extends Operation{

    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<Integer> cdisMapIdsToSync;  //list of all CDIS MAP IDs to sync
    
    private Table <String,String,Integer> columnLengthHashTable;  //List of columns with maximum length in DAMS
    
    private ArrayList<String> deleteRows;
    private ArrayList<String> deletesForUoiid;
 
    private HashMap<String, String[]> metaDataMapQueries;   //Queries to run from XML fuke with delimiter from XML file

    private Table <String, String,String> updateRowForDams;  //DAMS Table, DAMS Column, result value from CIS
    private HashMap<String, String> updatesByTableName; //DAMS Table Name and the actual sql to run 

    private Table <String, String,String> insertRowForDams;
    private HashMap<String, ArrayList<String>> insertsByTableName; 
    
    Connection sourceDb;
    
    public MetaDataSync() {
         
    }
    
    private void setSourceDb() {
        if (DamsTools.getProperty("cis").equals("cdisDb")  ) {
            sourceDb = DamsTools.getDamsConn();
        }
        else {
            sourceDb = DamsTools.getCisConn();
        }
    }
   
    private String calculateMaxIdsSize (String tmsRemarks) {
        
        String idsSize = null;
        String defaultIdsSize = "3000";
        
        try {
            Pattern p = Pattern.compile("MAX IDS SIZE = (\\d+)");
            Matcher m = p.matcher(tmsRemarks);
        
            if (m.find()) {
                idsSize = m.group(1);
                //logger.log(Level.SEVERE, "Size in TMS set to: " + m.group(1));
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
    
    
    private String calculateMaxIdsSize (String tmsRemarks, String idsType) {
        
        String idsSize = null;
        String defaultIdsSize = "3000";
        
        try {
            Pattern p = Pattern.compile("MAX "+ idsType +" IDS SIZE = (\\d+)");
            Matcher m = p.matcher(tmsRemarks);
        
            if (m.find()) {
                idsSize = m.group(1);
                //logger.log(Level.SEVERE, "Size in TMS set to: " + m.group(1));
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
    
    /*  Method :        buildCISQueryPopulateResults
        Arguments:      
        Returns:        true for success, false for failures
        Description:    creates the sql statements to run in the CIS, then executes them and put results in query structure
        RFeldman 2/2015
    */

    private boolean buildCisQueryPopulateResults(CdisMap cdisMap) {
        
        this.deleteRows = new ArrayList<>();
        this.insertRowForDams = HashBasedTable.create();
        this.updateRowForDams = HashBasedTable.create();
        
        //Loop through all of the queries for the current operation type
        for (String sql : this.metaDataMapQueries.keySet()) {
          
            String destTableName = metaDataMapQueries.get(sql)[0].toUpperCase();
            String operationType = metaDataMapQueries.get(sql)[1];
            String appendDelimter = metaDataMapQueries.get(sql)[2];
            
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
    
    
    /*  Method :        generateUpdate
        Arguments:      
        Returns:        true for success, false for failures
        Description:    generates the update sql for updating metadata in the DAMS
        RFeldman 2/2015
    */
    private boolean generateSql(String uoiId, boolean parentChildSync) {
            
        for (String tableName: deleteRows) {
            deletesForUoiid.add("DELETE FROM towner." + tableName + " WHERE UOI_ID = '" + uoiId + "'");
        }
        
        for (Cell<String, String, String> cell : this.insertRowForDams.cellSet()) {
            
            String tableName =  cell.getRowKey();
            String columnName = cell.getColumnKey();
            String value = cell.getValue();
            
            ArrayList<String> sqlVals = new ArrayList<String>();
            
            if (value.contains("^MULTILINE_LIST_SEP^")) {
                 // If the value contains "^MULTILINE_LIST_SEP^" then we need to break that result down and perform two or more insert statements 
                Pattern delim = Pattern.compile("^MULTILINE_LIST_SEP^");
                
                String valuesToInsert[] = value.split(delim.quote("^") );
                                                                 
                //We can have multiple inserts for a single table, the first column of the map holds the tablename,
                //the list contains the insert statements
                for (int i =0; i < valuesToInsert.length; i++ ) {
                    
                    if ( ! (valuesToInsert[i].equals("MULTILINE_LIST_SEP") ) ) {
                        sqlVals.add ("INSERT INTO towner." + tableName + 
                            " (UOI_ID, " + columnName + ") VALUES ('" + 
                            uoiId + "','" + valuesToInsert[i] + "')");
                    }
                    
                    insertsByTableName.put(tableName, sqlVals);
                
                }
            }
            else {
                //We only have a single row to insert into the table specified.
                sqlVals.add  ("INSERT INTO towner." + tableName + 
                    " (UOI_ID, " + columnName + ") VALUES ('" + 
                    uoiId + "','" + value + "')");
                
                insertsByTableName.put(tableName, sqlVals);
                
            }
                    
        }    
        
        for (Cell<String, String, String> cell : updateRowForDams.cellSet()) {
            
            String tableName =  cell.getRowKey();
            String columnName = cell.getColumnKey();
            String value = cell.getValue();
            
            //never update these special fields in parent/child sync
            if (parentChildSync) {
                switch (columnName) {
                    case "ADMIN_CONTENT_TYPE" :
                    case "IS_RESTRICTED" :
                    case "MANAGING_UNIT" :
                    case "MAX_IDS_SIZE" :
                    case "INTERNAL_IDS_SIZE" :
                    case "PUBLIC_USE" :                  
                    case "SEC_POLICY_ID" :
                    case "SI_DEL_RESTS" :
                        continue;
                }
            }
            
            if (updatesByTableName.isEmpty() || updatesByTableName.get(tableName) == null) {
                updatesByTableName.put(tableName, "UPDATE towner." + tableName + " SET " + columnName + "= '" + value + "'");
            }
            else {
                updatesByTableName.put(tableName, updatesByTableName.get(tableName) + ", " + columnName + "= '" + value + "'");
            }
        }   
        
        return true;
        
    }
    
    
    /*  Method :        performTransactions
        Arguments:      
        Returns:        true for success, false for failures
        Description:    transverses list of updates/inserts/deletes for metadata sync in DAMS and kicks them off one at a time.
        RFeldman 12/2016
    */
    
    private boolean performTransactions (String linkedOrParChilduoiId, CdisMap cdisMap, boolean parentChildOnly) {
        
        deletesForUoiid = new ArrayList<>();
        insertsByTableName = new HashMap<>();
        updatesByTableName =  new HashMap<>();
            
        boolean sqlUpdateCreated = generateSql(linkedOrParChilduoiId, parentChildOnly); 
        
        //Perform any deletes that need to be run on the current DAMSID
        for (String sqlToDelete :deletesForUoiid) {
            updateDamsData(sqlToDelete);
        }
            
        //Perform any Inserts that need to be run on the current DAMSID
        for (String tableName : insertsByTableName.keySet()) {
            ///NEED TO DO DB INSERTS!
            ArrayList<String> sqlToInsert = new ArrayList<String>();
            
            sqlToInsert = insertsByTableName.get(tableName);
            for (String sqlStmnt :sqlToInsert  ) {
                int insertCount = updateDamsData(sqlStmnt);
                
                if (insertCount != 1) {
                    ErrorLog errorLog = new ErrorLog ();
                    //Get CDISMAPID by uoiId
                    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to insert DAMS metadata " + linkedOrParChilduoiId);    
                    return false;    
                }
            }   
        }      
        
        //Perform any updates that need to be run on the current DAMSID
        for (String tableName : updatesByTableName.keySet()) {
            String sqlToUpdate = updatesByTableName.get(tableName);
            sqlToUpdate = sqlToUpdate + " WHERE uoi_id  = '" + linkedOrParChilduoiId + "'";
            int updateCount = updateDamsData(sqlToUpdate);
                
            // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
            if (updateCount != 1) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " + linkedOrParChilduoiId); 
                return false;
            } 
        }
    
        Uois uois = new Uois();
        uois.setUoiid(linkedOrParChilduoiId);
        if (DamsTools.getProperty("overrideUpdtDt") != null) {
            uois.setMetadataStateDt(DamsTools.getProperty("overrideUpdtDt"));
        }
        else {
            uois.setMetadataStateDt("SYSDATE");
        }
        
        int updateCount = uois.updateMetaDataStateDate();
        if (updateCount != 1) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMD", "Error, unable to update uois table with new metadata_state_dt " + linkedOrParChilduoiId);   
            return false; 
        }
        
        return true;
    }
    
    
    /*  Method :       populateCisUpdatedMapIds
        Arguments:      
        Description:    populates list of media records in the CDIS_MAP table by CDIS_MAP_ID that have been updated in the CIS system
        RFeldman 2/2015
    */
    private void populateCisUpdatedMapIds() {
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","getRecordsForResync");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql getRecordsForResync not found");
            return;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        setSourceDb();
        try (PreparedStatement stmt = sourceDb.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                CdisMap cdisMap = new CdisMap();
                    
                if (DamsTools.getProperty("cis").equals("aSpace")){
                    CdisCisGroupMap cdisCisGroupMap = new CdisCisGroupMap();
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
        }    
    }
    
    
    /*  Method :        populateColumnWidthArray
        Arguments:      
        Returns:        true for success, false for failures
        Description:    populates structure to hold columns and widths for all DAMS tables involved in metadata sync
        RFeldman 12/2016
    */
    private boolean populateColumnWidthArray (String destTableName) {
          
        String sql = "SELECT column_name, char_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + destTableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type in ('VARCHAR2','CHAR') " + 
                     "AND column_name NOT IN ('UOI_ID','OWNING_UNIT_UNIQUE_NAME')" +
                     "UNION " +
                     "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + destTableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type = 'NUMBER' " + 
                     "UNION " +
                     "SELECT column_name, 16 " +
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + destTableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type = 'DATE' ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs != null && rs.next()) {         
                columnLengthHashTable.put(destTableName, rs.getString(1), rs.getInt(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain data field lengths ", e);
        
        }
        return true;
    
    }
    
    
    /*  Method :       populateNeverSyncedMapIds
        Arguments:      
        Description:    populates list of media records in the CDIS_MAP table by CDIS_MAP_ID that have never been synced 
        RFeldman 2/2015
    */
    private void populateNeverSyncedMapIds() {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList() ) {
            sql = xmlInfo.getDataForAttribute("type","getNeverSyncedRecords");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {
                
            while (rs.next()) {

                logger.log(Level.ALL, "Adding to list to sync: " + rs.getInt(1));
                cdisMapIdsToSync.add(rs.getInt("CDIS_MAP_ID"));
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error obtaining list to sync", e);
            return;
        }
    }
    
    
    /*  Method :       populateCisUpdatedMapIds
        Arguments:      
        Description:    Helps generate the update statement used for the metadata sync by determining if we should add onto existing update
                        statement or start a new update statement 
        RFeldman 12/2016
    */
    private boolean populateUpdateRowForDams (String tableName, String columnName, String resultVal, String appendDelim) {
                    
        //check if the column has already been populated
        String priorResults = updateRowForDams.get(tableName, columnName);
        
        if (priorResults != null) {
            if (appendDelim != null ) {
            //append to existing column
            updateRowForDams.put(tableName, columnName, priorResults + appendDelim + ' ' + resultVal);
            }
            else {
                //put out error
                logger.log(Level.ALL, "Warning: Select statement expected to return single row, returned multiple rows. populating with only one value");
                updateRowForDams.put(tableName, columnName, resultVal);
            }
        }
        else {
            if (resultVal == null) {
                updateRowForDams.put(tableName, columnName, "null");
            }    
            else {
                updateRowForDams.put(tableName, columnName, resultVal);
            }
        }
        
        return true;
    }
        
    
    /*  Method :        populateXmls
        Arguments:      
        Returns:        true for success, false for failures
        Description:    populates structure to hold columns and widths for all DAMS tables involved in metadata sync
        RFeldman 12/2016
    */
    private void populateXmls() {
        
        this.metaDataMapQueries = new HashMap<>();
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            if (! xmlInfo.getAttributeData("type").equals("metadataMap")) {    
                continue;
            }
   
            sql = xmlInfo.getDataValue();
    
            logger.log(Level.FINEST, "SQL: {0}", sql); 
            
            if (sql != null) {
                String[] tableNameDelim = new String[3];
                tableNameDelim[0] = xmlInfo.getAttributeData("destTableName");
                tableNameDelim[1]= xmlInfo.getAttributeData("operationType");
                tableNameDelim[2]= xmlInfo.getAttributeData("multiResultDelim");  
            
                metaDataMapQueries.put(sql, tableNameDelim);
                   
            }
        }
        if (metaDataMapQueries == null) {
            logger.log(Level.FINEST, "Error: Required sql metadataMap not found");
            return;
        }
       
  
    }
    
    
    /*  Method :       processListToSync
        Arguments:      
        Description:   The main 'driver' for the record level. Goes through the list of media Records that were determined to require syncing one at a time 
      syncParentChild  RFeldman 2/2015
    */
    private void processListToSync() {

        // for each UOI_ID that was identified for sync
        for (Iterator<Integer> iter = cdisMapIdsToSync.iterator(); iter.hasNext();) {
            
            boolean noErrorFound = true;
            
            //commit with each iteration
            try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            CdisMap cdisMap = new CdisMap();
            MediaRecord mediaRecord = new MediaRecord();
                
            cdisMap.setCdisMapId(iter.next());
            cdisMap.populateMapInfo();

            //Add the current
            if (DamsTools.getProperty("syncParentChild") != null  && DamsTools.getProperty("syncParentChild").equals("true") ) {
                mediaRecord.setUoiId(cdisMap.getDamsUoiid()); 
                mediaRecord.buildDamsRelationList ();
            }
            
            CDISUpdates cdisUpdates = new CDISUpdates();
            //For ArchiveSpace, we need to prep the view that we get data from 
            if (DamsTools.getProperty("cis").equals("aSpace")){
                
                CdisCisGroupMap cdisCisGroupMap = new CdisCisGroupMap();
                cdisCisGroupMap.setCdisMapId(cdisMap.getCdisMapId());
                cdisCisGroupMap.setCisGroupCd("ead");
                cdisCisGroupMap.populateCisGroupValueForCdisMapIdType();
                cdisUpdates.setEadRefId(cdisCisGroupMap.getCisGroupValue());
                
                boolean getDescriptiveDateCalled = cdisUpdates.callGetDescriptiveData();
                
                if (getDescriptiveDateCalled != true) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to Seed Archive Space information " + cdisMap.getDamsUoiid());   
                    noErrorFound = false;
                    continue; 
                }  
            }
            
                
             // execute the SQL statment to obtain the metadata and populate variables. The key value is the CDIS MAP ID
            boolean dataMappedFromCIS = buildCisQueryPopulateResults(cdisMap);
            if (! dataMappedFromCIS) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
                noErrorFound = false;
                continue; 
            }
            
            noErrorFound = performTransactions(cdisMap.getDamsUoiid(), cdisMap, false);
            
            if (! noErrorFound) {
               continue;
            }
            
            if (mediaRecord.relatedUoiIds != null) {
                for (String relatedUoiId : mediaRecord.relatedUoiIds) {
                    noErrorFound = performTransactions(relatedUoiId, cdisMap, true);
                }
            }
            
            if (! noErrorFound) {
               continue;
            } 
            
            // see if there already is a row that in the activity_log that has been synced
            CdisActivityLog cdisActivity = new CdisActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MDS");
        
            boolean idFound = cdisActivity.populateIdFromMapIdStat();
            if (! idFound ) {                               
                 // Insert row into activity_log table
                 boolean activityLogged = cdisActivity.insertActivity();
                 if (!activityLogged) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "CRACTL",  "Could not create CDIS Activity entry: " + cdisMap.getDamsUoiid());  
                    continue;
                }
            }
            else {
                // Update the date in activity_log table on the existing row
                boolean activityLogged = cdisActivity.updateActivityDtCurrent();
                    
                if (!activityLogged) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "CRACTL",  "Could not update CDIS Activity entry: " + cdisMap.getDamsUoiid());
                    continue;
                }    
            }

        }
    }
    
    
    /*  Method :       scrubString
        Arguments:      
        Description:   Scrubs the database insert/update/delete statement and replaces special characters before updating the DAMS database
      syncParentChild  RFeldman 2/2015
    */
    public String scrubString(String inputString) {
          
        String newString;
        
        // remove & for easy insert into db
        //newString = inputString.replaceAll("&", "and");
	
        //substitute any 'right' apostrophes to a pair of single quotes
        newString = inputString.replaceAll("\u2019", "'");
        
        //substitute 'em and en dash' for regular dash 
        newString = newString.replaceAll("\u2012", "-");
        newString = newString.replaceAll("\u2013", "-");
        
        //substitute curly double quotes for regular double quotes
        newString = newString.replaceAll("\u201c", "\"");
        newString = newString.replaceAll("\u201d", "\"");
        
        newString = newString.replaceAll("\r\n", "\n");
        
	//double any single quotes
	newString = newString.replaceAll("'", "''");
        
        // remove leading and trailing spaces
        newString = newString.trim();
        
        return newString;
        
    }
    
    /*  Method :        sync
        Arguments:      
        Description:    The main driver for the sync operation Type 
        RFeldman 2/2015
    */
    public void invoke() {
        
        // initialize uoiid list for sync
        cdisMapIdsToSync = new ArrayList<>();
        
        // Grab all the records that have NEVER EVER been synced by CDIS yet
        populateNeverSyncedMapIds();      
        
        //Grab all the records that have been synced in the past, but have been updated in the CIS
        //  Must check first, some implementations do not even have a CIS to check
        if (! DamsTools.getProperty("cis").equals("none")) {
            populateCisUpdatedMapIds();
        }

        if (cdisMapIdsToSync.isEmpty()) {
            logger.log(Level.FINEST,"No Rows found to sync");
            return;
        }
        
        populateXmls();
        if (metaDataMapQueries.isEmpty()) {
            logger.log(Level.FINEST,"unable to find metadata sync xmls");
            return;
        }
        
        //For now, populate the 4 tables we are syncing to, maybe at a later point we can have the code look for the tables in the SQL
        this.columnLengthHashTable = HashBasedTable.create();
        populateColumnWidthArray("SECURITY_POLICY_UOIS");
        populateColumnWidthArray("SI_ASSET_METADATA");
        populateColumnWidthArray("SI_AV_ASSET_METADATA");
        populateColumnWidthArray("SI_MANAGING_UNIT_DTLS");
        populateColumnWidthArray("SI_NAMED_PERSON_DTLS");
        populateColumnWidthArray("SI_PRESERVATION_METADATA");
        populateColumnWidthArray("SI_RESTRICTIONS_DTLS");
        
        processListToSync ();
        
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
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
    
}
