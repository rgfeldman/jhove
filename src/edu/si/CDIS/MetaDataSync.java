
package edu.si.CDIS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
import edu.si.CDIS.utilties.ScrubStringForDb;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.utilties.ErrorLog;

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
                e.printStackTrace();
        } 
        
        return recordsUpdated;

    }

    /*  Method :       getNeverSyncedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private void getCISUpdatedRendition() {
        String sqlTypeArr[] = null;
        String sql = null;

        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr =  CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getRecordsForResync")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
            ResultSet  rs = pStmt.executeQuery() ) {
            
            CDISMap cdisMap = new CDISMap();
            
            logger.log(Level.FINEST,"SQL! " + sql); 
                 
            while (rs.next()) {
                logger.log(Level.ALL, "Adding RenditionID to re-sync list: " + rs.getString(1));
                cdisMap.setCisUniqueMediaId(rs.getString(1));
                boolean cisMediaIdObtained = cdisMap.populateIdFromCisMediaId();
                
                //check to make sure we were able to get the CDIS_MAP record
                if (! cisMediaIdObtained) {
                    logger.log(Level.FINEST,"Media not tracked by CDIS or errored.  CIS_MediaID: " + rs.getString(1));
                    continue;
                }

                //Only add to the list if it is not already in the list. It could be there from never synced record list
                if (!cdisMapIdsToSync.contains(cdisMap.getCdisMapId())) {
                    cdisMapIdsToSync.add(cdisMap.getCdisMapId());
                }

            }

        } catch (Exception e) {
            logger.log(Level.ALL, "Error in generation list to sync: " + e);
        } 
        
        
    }
    
    /*  Method :       getNeverSyncedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private void getNeverSyncedRendition() {

        String sqlTypeArr[] = null;
        String sql = null;

        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr =  CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getNeverSyncedRecords")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet  rs = pStmt.executeQuery() ) {
            
            logger.log(Level.FINEST,"SQL! " + sql); 
                 
            while (rs.next()) {
                logger.log(Level.ALL, "Adding to list to sync: " + rs.getInt(1));
                cdisMapIdsToSync.add(rs.getInt("CDIS_MAP_ID"));
            }

        } catch (Exception e) {
            logger.log(Level.ALL, "Error in generation list to sync: " + e);
        } 
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

            try {
                
                cdisMap.setCdisMapId(iter.next());
                cdisMap.populateMapInfo();
                siAsst.setUoiid (cdisMap.getDamsUoiid());
                
                // execute the SQL statment to obtain the metadata and populate variables.  They key value is RenditionID
                boolean dataMappedFromCIS = mapData(cdisMap);
                if (! dataMappedFromCIS) {
                    throw new Exception ();
                }
                
                //generate the update statement from the variables obtained in the mapData
                generateUpdate(siAsst);
 
                // Perform the update to the Dams Data
                int updateCount = updateDamsData();
                
                // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
                if (updateCount < 1) {
                    throw new Exception (); 
                }
                else if (updateCount > 1) {
                    logger.log(Level.ALL, "Warning, updated multiple rows of metadata for uoi_id: " + cdisMap.getDamsUoiid());
                }
                
                Uois uois = new Uois();
                uois.setUoiid(cdisMap.getDamsUoiid());
                updateCount = uois.updateMetaDataStateDate();
                if (updateCount < 1) {
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
                
                        
            } catch (Exception e) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPDAMM", "Error, unable to update Dams with new Metadata " + cdisMap.getDamsUoiid());   
            }
        }
    }
    
    
    /*  Method :        generateUpdate
        Arguments:      
        Returns:      
        Description:    generates the update sql for updating metadata in the DAMS SI_ASSET_METADATA table
        RFeldman 2/2015
    */
    private boolean generateUpdate(SiAssetMetaData siAsst) {

        // Go through the list and 
        try {
            boolean firstIteration = true;
             
            String updateStatement = "UPDATE towner.SI_ASSET_METADATA SET ";
                                             
            for (String column : metaDataValuesForDams.keySet()) {
                
                String columnValue = metaDataValuesForDams.get(column);
            
                if (columnValue != null) {
                
                    //Integer maxColumnLength = siAsst.metaDataDBLengths.get(column);
                    Integer maxColumnLength = siAsst.metaDataDBLengths.get(column);
                
                    if (columnValue.length() > siAsst.metaDataDBLengths.get(column)) { 
                            columnValue = columnValue.substring(0,maxColumnLength);
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
        String sql;
        String sqlTypeArr[];
        String sqlType;
        String delimiter;
        
        ScrubStringForDb scrub = new ScrubStringForDb();

        logger.log(Level.ALL, "Mapping Data for metadata");
        
        this.metaDataValuesForDams = new HashMap <>();
        
        //for each select statement found in the xml 
        for (String key : CDIS.getXmlSelectHash().keySet()) {

            // Get the sql value from the hasharray
            sql = key;
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            sqlType = sqlTypeArr[0];
            delimiter = sqlTypeArr[1];
            
            if (! ((sqlTypeArr[0].equals("singleResult")) || (sqlTypeArr[0].equals("cursorAppend")))  ){      
                //get the next query, we are not interested in this one at this point
                continue;
            }
                      
            if (sql.contains("?UOI_ID?")) {
                sql = sql.replace("?UOI_ID?", String.valueOf(cdisMap.getDamsUoiid()));
            }
            if (sql.contains("?MEDIA_ID?")) {
                sql = sql.replace("?MEDIA_ID?", String.valueOf(cdisMap.getCisUniqueMediaId()));
            }
            if (sql.contains("?OBJECT_ID?")) {
                CDISObjectMap objectMap = new CDISObjectMap();
                objectMap.setCdisMapId(cdisMap.getCdisMapId());
                objectMap.populateCisUniqueObjectIdforCdisId();
           
                sql = sql.replace("?OBJECT_ID?", String.valueOf(objectMap.getCisUniqueObjectId()));
            }
                        
            logger.log(Level.ALL, "select Statement: " + sql);    
            
            try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
                 ResultSet rs = pStmt.executeQuery() ) {
                
                while (rs.next()) {
                    
                    try {
                    
                        ResultSetMetaData rsmd = rs.getMetaData();
                    
                        for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                            String columnName = rsmd.getColumnName(i);
                            String columnValue = rs.getString(i);
                        
                            logger.log(Level.ALL, "columnInfo: " + columnName + " " + columnValue);
                        
                            if (columnValue != null) {
                                columnValue = scrub.scrubString(columnValue);
                            }
                    
                            //check if the column has already been populated
                            String columnExisting = metaDataValuesForDams.get(columnName);
                            if (columnExisting != null) {
                                if (sqlType.equals("cursorAppend")) {
                                    //append to existing column
                                    metaDataValuesForDams.put(columnName.toLowerCase(), columnExisting + delimiter + ' ' + columnValue);
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
                    } catch (Exception e) {
                        logger.log(Level.ALL, "Error, exception raised in metadata while loop", e); 
                        return false;
                    }
                }
            } catch (Exception e) {
                logger.log(Level.ALL, "Error, exception raised in metadata for loop", e); 
                return false;
            } 
            
        }
        return true;
    }
}
