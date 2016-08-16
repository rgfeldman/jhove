
package edu.si.CDIS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.DAMS.Database.TeamsLinks;
import edu.si.CDIS.DAMS.Database.SiRestrictionsDtls;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.CDIS.utilties.ScrubStringForDb;
import edu.si.Utils.XmlSqlConfig;

public class MetaDataSync {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String sqlUpdate;
    private HashMap <String,String> metaDataValuesForDams; 
    private ArrayList<Integer> cdisMapIdsToSync;

    private void setSqlUpdate(String sqlUpdate) {
        this.sqlUpdate = sqlUpdate;
    }

    private String getSqlUpdate() {
        return this.sqlUpdate;
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

        SiAssetMetaData siAsst = new SiAssetMetaData ();        
        
        if (! CDIS.getProperty("cisSourceDB").equals("none")) {
            //Grab all the records that have been synced in the past, but may have updates      
            getCISUpdatedRendition();
        }

        if (!cdisMapIdsToSync.isEmpty()) {
            //Populate column definitions and length array
            siAsst.populateMetaDataDBLengths ();
        
            processRenditionList(siAsst);
        }
    }

    /*  Method :        updateDamsData
        Arguments:      
        Description:    Updates the DAMS with the metadata changes 
        RFeldman 2/2015
    */
    private int updateDamsData() {

        Integer recordsUpdated = 0;
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(getSqlUpdate())) {
 
            recordsUpdated = pStmt.executeUpdate(getSqlUpdate());
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating DAMS data", e);
            return -1;    
        } 
        
        return recordsUpdated;

    }

    /*  Method :       getNeverSyncedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private void getCISUpdatedRendition() {

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
    
    private boolean performUpdates (SiAssetMetaData siAsst, CDISMap cdisMap, boolean managedByCIS) {
        
        //generate the update statement from the variables obtained in the mapData
        generateUpdate(siAsst, managedByCIS);
 
        // Perform the update to the Dams Data
        int updateCount = updateDamsData();
                
        // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
        if (updateCount < 1) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update DAMS metadata " + cdisMap.getDamsUoiid());    
            return false;
        }
        else if (updateCount > 1) {
            logger.log(Level.ALL, "Warning, updated multiple rows of metadata for uoi_id: " + siAsst.getUoiid());
        }
                
        Uois uois = new Uois();
        uois.setUoiid(siAsst.getUoiid());
        updateCount = uois.updateMetaDataStateDate();
        if (updateCount < 1) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMD", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
            return false; 
        }
                
        // see if there already is a row that in the activity_log that has been synced
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        if (managedByCIS) {
            cdisActivity.setCdisStatusCd("MDS");
        }
        else {
            cdisActivity.setCdisStatusCd("MDP");
        }
        
        boolean idFound = cdisActivity.populateIdFromMapIdStat();
        if (! idFound ) {                               
            // Insert row into activity_log table
            boolean activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRACTL",  "Could not create CDIS Activity entry: " + cdisMap.getDamsUoiid());   
                return false;
            }
        }
        else {
            // Update the date in activity_log table on the existing row
            boolean activityLogged = cdisActivity.updateActivityDtCurrent();
                    
            if (!activityLogged) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRACTL",  "Could not update CDIS Activity entry: " + cdisMap.getDamsUoiid());   
                return false;
            }    
        }
        
        return true;
        
    }
    
    
    /*  Method :       processRenditionList
        Arguments:      
        Description:    Goes through the list of rendition Records one at a time 
                        and determines how to update the metadata on each one
        RFeldman 2/2015
    */
    private void processRenditionList(SiAssetMetaData siAsst) {

        // for each UOI_ID that was identified for sync
        for (Iterator<Integer> iter = cdisMapIdsToSync.iterator(); iter.hasNext();) {
            
            //commit with each iteration
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            CDISMap cdisMap = new CDISMap();
                
            cdisMap.setCdisMapId(iter.next());
            cdisMap.populateMapInfo();
            siAsst.setUoiid (cdisMap.getDamsUoiid());
                
             // execute the SQL statment to obtain the metadata and populate variables.  They key value is RenditionID
            boolean dataMappedFromCIS = mapData(cdisMap);
            if (! dataMappedFromCIS) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
                continue; 
            }
                
            boolean rowUpdated = performUpdates(siAsst, cdisMap, true);
                
            if (! rowUpdated) {
                //go to the next record, no need to sync children records, error should already be recorded
                continue;
            } 
                
            if (CDIS.getProperty("syncDamsChildren") == null  || CDIS.getProperty("syncDamsChildren").equals("false") ) {
                //skip next steps, they are only for syncing children records
                continue;
            }
            
            // See if there are any related parent/children relationships in DAMS. We find the parents whether they were put into DAMS
            // by CDIS or not.  We get only the direct parent for now...later we may want to add more functionality
            TeamsLinks teamsLinks = new TeamsLinks();
            teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
            teamsLinks.setLinkType("CHILD");
            boolean parentRetrieved = teamsLinks.populateDestValue();
                
            //Only update a parent record if we found one, or else just skip this step.
            if (! parentRetrieved) {
                logger.log(Level.ALL, "Error: unable to obtain parent id to sync");
                continue;
            }
            if (teamsLinks.getDestValue() == null) {
                logger.log(Level.ALL, "No parent id to sync");
                continue;
            }
                
            // set the current uoiid to what we obtained in the teams_links table for update
            siAsst.setUoiid (teamsLinks.getDestValue());
                
            //For each parent or child, generate update statement.  should be the same, except certain fields we do not update
            //for parent/children.  These include public_use, max_ids_size, is_restricted
            performUpdates(siAsst, cdisMap, false);
        }
    }
    
    private void handleRestrictions (String uoiId, String restrictionList) {
        //This is not in siAssetMetadata and needs to be handled differently
        SiRestrictionsDtls siRestictionsDtls = new SiRestrictionsDtls();
        
        siRestictionsDtls.setUoiId(uoiId);
        
        // delete restrictions if they exist we will put new ones in.  if they dont exist this is fine too.
        siRestictionsDtls.deleteRestrictions();
        
        if (restrictionList != null ) {
            //Loop through list and insert new restriction
            String[] restrictionArray = restrictionList.split(",");
         
            for (int i = 0; i < restrictionArray.length; i++) {
            siRestictionsDtls.setRestrictions(restrictionArray[i]);
            siRestictionsDtls.insertRestrictions();
            }
        }
    }
    
    
    /*  Method :        generateUpdate
        Arguments:      
        Returns:      
        Description:    generates the update sql for updating metadata in the DAMS SI_ASSET_METADATA table
        RFeldman 2/2015
    */
    private boolean generateUpdate(SiAssetMetaData siAsst, boolean managedByCIS) {

        // Go through the list and 
        try {
            boolean firstIteration = true;
             
            String updateStatement = "UPDATE towner.SI_ASSET_METADATA SET ";
            
            for (String column : metaDataValuesForDams.keySet()) {
                
                String columnValue = metaDataValuesForDams.get(column);
                
                if (column.equals("RESTRICTIONS") ) {
                    handleRestrictions(siAsst.getUoiid(), metaDataValuesForDams.get(column));
                    continue;
                }
                                
                if (columnValue != null) {
                
                    Integer maxColumnLength = siAsst.metaDataDBLengths.get(column);
                
                    if (columnValue.length() > siAsst.metaDataDBLengths.get(column)) { 
                            columnValue = columnValue.substring(0,maxColumnLength);
                    }  
                } 
                
                //Skip certain fields in metadata update if this is only based on parent record
                if (! managedByCIS) {
                    switch (column) {     
                        case "PUBLIC_USE" :
                        case "MAX_IDS_SIZE" :
                        case "IS_RESTRICTED" :
                        case "ADMIN_CONTENT_TYPE" :    
                            continue;
                    }
                }
                
                if (! firstIteration) {
                    updateStatement = updateStatement + ", ";
                }
                else {
                    firstIteration = false;
                }
                
                updateStatement = updateStatement + " " + column + " = '" + columnValue + "'";       
            }

            updateStatement = updateStatement +
                " WHERE uoi_id = '" + siAsst.getUoiid() + "'";

            //we collected nulls in the set/get commands.  strip out the word null from the update statement
            updateStatement = updateStatement.replaceAll("null", "");

            setSqlUpdate(updateStatement);

            logger.log(Level.ALL, "updateStatment: " + updateStatement);
        
            return true;
        
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }

    }
    
    /*  Method :        mapData
        Arguments:      
        Returns:      
        Description:    maps the information obtained from the TMS select statement to the appropriate member variables
        RFeldman 2/2015
    */

    private boolean mapData(CDISMap cdisMap) {
        
        ScrubStringForDb scrub = new ScrubStringForDb();

        logger.log(Level.ALL, "Mapping Data for metadata");
        
        this.metaDataValuesForDams = new HashMap <>();
        
        //for each select statement found in the xml 
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("metadataMap"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
            
            String sql = xml.getSqlQuery();   
            if (sql.contains("?MEDIA_ID?")) {
                sql = sql.replace("?MEDIA_ID?", String.valueOf(cdisMap.getCisUniqueMediaId()));
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
                            String columnName = rsmd.getColumnName(i);
                            String columnValue = rs.getString(i);
                        
                            logger.log(Level.ALL, "columnInfo: " + columnName + " " + columnValue);
                        
                            if (columnValue != null) {
                                columnValue = scrub.scrubString(columnValue);
                            }
                    
                            //check if the column has already been populated
                            String columnExisting = metaDataValuesForDams.get(columnName.toUpperCase());
                             
                            if (columnExisting != null) {
                                if (! xml.getMultiResultDelim().isEmpty() ) {
                                    //append to existing column
                                    metaDataValuesForDams.put(columnName.toUpperCase(), columnExisting + xml.getMultiResultDelim() + ' ' + columnValue);
                                }
                                else {
                                    //put out error
                                    logger.log(Level.ALL, "Warning: Select statement expected to return single row, returned multiple rows. populating with only one value");
                                    metaDataValuesForDams.put(columnName.toUpperCase(),columnValue);
                                }                            
                            }
                            else {
                                metaDataValuesForDams.put(columnName.toUpperCase(),columnValue);
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
}
