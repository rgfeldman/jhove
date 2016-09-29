
package edu.si.CDIS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;


import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.DAMS.Database.TeamsLinks;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.Utils.XmlSqlConfig;


public class MetaDataSync {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList<Integer> cdisMapIdsToSync;  //list of all CDIS MAP IDs to sync
    private Table <String,String,Integer> columnLengthHashTable;  //List of columns with maximum length in DAMS
    
    private ArrayList<String> deleteRows;
    private ArrayList<String> deletesForUoiid;
 
    private HashMap<String, String> metaDataMapQueries;   //Queries to run from XML fuke with delimiter from XML file

    private Table <String, String,String> updateRowForDams;  //DAMS Table, DAMS Column, result value from CIS
    private HashMap<String, String> updatesByTableName; //DAMS Table Name and the actual sql to run 

    private Table <String, String,String> insertRowForDams;
    private HashMap<String, String> insertsByTableName; 


    /*  Method :        generateUpdate
        Arguments:      
        Returns:      
        Description:    generates the update sql for updating metadata in the DAMS SI_ASSET_METADATA table
        RFeldman 2/2015
    */
    private boolean generateSql(String uoiId, boolean SyncFromParentChild) {
            
        deletesForUoiid = new ArrayList<>();
        for (String tableName: deleteRows) {
            deletesForUoiid.add("DELETE FROM towner." + tableName + " WHERE UOI_ID = '" + uoiId + "'");
        }
        
        insertsByTableName = new HashMap<>();
        for (Cell<String, String, String> cell : this.insertRowForDams.cellSet()) {
            
            String tableName =  cell.getRowKey();
            String columnName = cell.getColumnKey();
            String value = cell.getValue();
            
            if (insertsByTableName.isEmpty() || insertsByTableName.get(tableName) == null) {
                insertsByTableName.put(tableName, 
                    "INSERT INTO towner." + tableName + 
                    " (UOI_ID, " + columnName + ") VALUES ('" + 
                    uoiId + "','" + value + "'");
            }
            else {
                insertsByTableName.put(tableName, insertsByTableName.get(tableName) + ", " + columnName + "= '" + value + "'");
            }
        }    
        
        updatesByTableName =  new HashMap<>();
        for (Cell<String, String, String> cell : updateRowForDams.cellSet()) {
            
            String tableName =  cell.getRowKey();
            String columnName = cell.getColumnKey();
            String value = cell.getValue();
            
            if (updatesByTableName.isEmpty() || updatesByTableName.get(tableName) == null) {
                updatesByTableName.put(tableName, "UPDATE towner." + tableName + " SET " + columnName + "= '" + value + "'");
            }
            else {
                updatesByTableName.put(tableName, updatesByTableName.get(tableName) + ", " + columnName + "= '" + value + "'");
            }
        }   
        
        return true;

    }
    
    
    /*  Method :       getNeverSyncedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private void getNeverSyncedRendition() {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getNeverSyncedRecords"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength() ; s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(xml.getSqlQuery());
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
    }
    
    /*  Method :       getUpdatedCisRecords
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private void getUpdatedCisRecords() {

        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getRecordsForResync"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getCisConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                while (rs.next()) {
                    CDISMap cdisMap = new CDISMap();
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
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to re-sync", e);
                return;
            }
        }
        
    }
    
    
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
        
    /*  Method :        mapData
        Arguments:      
        Returns:      
        Description:    maps the information obtained from the TMS select statement to the appropriate member variables
        RFeldman 2/2015
    */

    private boolean populateMetaDataValuesForDams(CDISMap cdisMap) {
        
        this.deleteRows = new ArrayList<>();
        this.insertRowForDams = HashBasedTable.create();
        this.updateRowForDams = HashBasedTable.create();
        
        //Loop through all of the queries for the current operation type
        for (String sql : this.metaDataMapQueries.keySet()) {
          
            String appendDelimter = metaDataMapQueries.get(sql);
            
            if (sql.contains("?MEDIA_ID?")) {
                sql = sql.replace("?MEDIA_ID?", String.valueOf(cdisMap.getCisUniqueMediaId()));
            }
            if (sql.contains("?UOI_ID?")) {
                sql = sql.replace("?UOI_ID?", String.valueOf(cdisMap.getDamsUoiid()));
            }
            if (sql.contains("?OBJECT_ID?")) {
                CDISObjectMap objectMap = new CDISObjectMap();
                objectMap.setCdisMapId(cdisMap.getCdisMapId());
                objectMap.populateCisUniqueObjectIdforCdisId();
           
                sql = sql.replace("?OBJECT_ID?", String.valueOf(objectMap.getCisUniqueObjectId()));
            }
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            try (PreparedStatement stmt = CDIS.getCisConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

                while (rs.next()) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    
                    for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                        String columnTableName[] = rsmd.getColumnName(i).split(" ");
                        String tableName = columnTableName[0].toUpperCase();
                        String columnName = columnTableName[1].toUpperCase();
                        String operationType = columnTableName[2];
                    
                        String resultVal = rs.getString(i);
                        
                        logger.log(Level.ALL, "TABL: " + tableName + " COLM: " + columnName + " VAL: " + resultVal);
                        
                        if (resultVal != null) {
                            //scrub the string to get rid of special characters that may not display properly in DAMS
                            resultVal = scrubString(resultVal);
                            
                           //Truncate the string if the length of the string exceeds the DAMS column width
                           if (resultVal.length() > columnLengthHashTable.get(tableName, columnName)) {
                                resultVal = resultVal.substring(0,columnLengthHashTable.get(tableName, columnName));
                           }
                        }
                    
                        switch (operationType) {
                            case "U":
                                populateUpdateRowForDams(tableName, columnName, resultVal, appendDelimter);
                                break;
                            case "DI":
                                deleteRows.add(tableName);
                                if (resultVal != null) {
                                    insertRowForDams.put(tableName, columnName, resultVal);
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
    
    
    
    private boolean populateColumnWidthArray (String destTableName) {
          
        String sql = "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + destTableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type != 'DATE' " + 
                     "AND column_name NOT IN ('UOI_ID','OWNING_UNIT_UNIQUE_NAME')" +
                     "UNION " +
                     "SELECT column_name, 16 " +
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + destTableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type = 'DATE' ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs != null && rs.next()) {         
                columnLengthHashTable.put(destTableName, rs.getString(1), rs.getInt(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain data field lengths ", e);
        
        }
        return true;
    
    }
    
    
    
    private void populateXmls() {
        
        this.metaDataMapQueries = new HashMap<>();
                
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("metadataMap"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength() ; s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
            
            logger.log(Level.FINEST, "Adding SQL to ArrayList: {0}", xml.getSqlQuery());
            
            metaDataMapQueries.put(xml.getSqlQuery(), xml.getMultiResultDelim());
            
        }
    }
    
    
    
    /*  Method :       processListToSync
        Arguments:      
        Description:    Goes through the list of rendition Records one at a time 
                        and determines how to update the metadata on each one
        RFeldman 2/2015
    */
    private void processListToSync() {

        // for each UOI_ID that was identified for sync
        for (Iterator<Integer> iter = cdisMapIdsToSync.iterator(); iter.hasNext();) {
            
            //commit with each iteration
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            CDISMap cdisMap = new CDISMap();
                
            cdisMap.setCdisMapId(iter.next());
            cdisMap.populateMapInfo();
                
             // execute the SQL statment to obtain the metadata and populate variables. The key value is the CDIS MAP ID
            boolean dataMappedFromCIS = populateMetaDataValuesForDams(cdisMap);
            if (! dataMappedFromCIS) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
                continue; 
            }
            
            boolean sqlUpdateCreated = generateSql(cdisMap.getDamsUoiid(), false);
            
            //Perform any deletes that need to be run on the current DAMSID
            for (String sqlToDelete :deletesForUoiid) {
                 updateDamsData(sqlToDelete);
            }
            
            //Perform any Inserts that need to be run on the current DAMSID
            for (String tableName : insertsByTableName.keySet()) {
                ///NEED TO DO DB INSERTS!
                String sqlToInsert = insertsByTableName.get(tableName);
                int insertCount = updateDamsData(sqlToInsert);
                
                if (insertCount != 1) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " + cdisMap.getDamsUoiid());    
                    continue;
                }
                                
            }
              
            //Perform any updates that need to be run on the current DAMSID
            for (String tableName : updatesByTableName.keySet()) {
                String sqlToUpdate = updatesByTableName.get(tableName);
                sqlToUpdate = sqlToUpdate + " WHERE uoi_id  = '" + cdisMap.getDamsUoiid() + "'";
                int updateCount = updateDamsData(sqlToUpdate);
                
                // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
                if (updateCount != 1) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " + cdisMap.getDamsUoiid());    
                    continue;
                }
            
            }
            
            Uois uois = new Uois();
            uois.setUoiid(cdisMap.getDamsUoiid());
            int updateCount = uois.updateMetaDataStateDate();
            if (updateCount != 1) {
               ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMD", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
                continue; 
            }
                
            // see if there already is a row that in the activity_log that has been synced
            CDISActivityLog cdisActivity = new CDISActivityLog();
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
                
            //if (CDIS.getProperty("syncDamsChildren") == null  || CDIS.getProperty("syncDamsChildren").equals("false") ) {
            //skip next steps, they are only for syncing children records
            //   continue;
            //}
            
            // See if there are any related parent/children relationships in DAMS. We find the parents whether they were put into DAMS
            // by CDIS or not.  We get only the direct parent for now...later we may want to add more functionality
            //TeamsLinks teamsLinks = new TeamsLinks();
            //teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
            //teamsLinks.setLinkType("CHILD");
            //boolean parentRetrieved = teamsLinks.populateDestValue();
                
                
                                
            //Skip certain fields in metadata update if this is only based on parent record
            //if (! SyncFromParentChild) {
            //    switch (column) {     
            //        case "PUBLIC_USE" :
            //        case "MAX_IDS_SIZE" :
            //        case "IS_RESTRICTED" :
            //        case "ADMIN_CONTENT_TYPE" :    
            //            continue;
            //    }
            //}
                
            //Only update a parent record if we found one, or else just skip this step.
            //if (! parentRetrieved) {
            //    logger.log(Level.ALL, "Error: unable to obtain parent id to sync");
            //   continue;
            //}
            //if (teamsLinks.getDestValue() == null) {
            //    logger.log(Level.ALL, "No parent id to sync");
            //    continue;
            //}
                
            //For each parent or child, generate update statement.  should be the same, except certain fields we do not update
            //for parent/children.  These include public_use, max_ids_size, is_restricted
            //performUpdates(siAsst, cdisMap, false);
            

        }
    }
    
    
    
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
    public void sync() {

        // initialize uoiid list for sync
        cdisMapIdsToSync = new ArrayList<>();
        
        // Grab all the records that have NEVER EVER been synced by CDIS yet
        getNeverSyncedRendition();      
        
        //Grab all the records that have been synced in the past, but have been updated in the CIS
        //  Must check first, some implementations do not even have a CIS to check
        if (! CDIS.getProperty("cisSourceDB").equals("none")) {
            getUpdatedCisRecords();
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
        populateColumnWidthArray("SI_ASSET_METADATA");
        populateColumnWidthArray("SI_AV_ASSET_METADATA");
        populateColumnWidthArray("SI_PRESERVATION_METADATA");
        populateColumnWidthArray("SI_RESTRICTIONS_DTS");
        
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
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sqlUpdate)) {
 
            recordsUpdated = pStmt.executeUpdate(sqlUpdate);
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating DAMS data", e);
            return -1;    
        } 
        
        return recordsUpdated;

    }
}
