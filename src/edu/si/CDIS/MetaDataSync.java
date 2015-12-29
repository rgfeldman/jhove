
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
import edu.si.CDIS.utilties.ScrubStringForDb;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.utilties.ErrorLog;

public class MetaDataSync {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String sqlUpdate;
    private HashMap <String,String> metaDataValuesForDams; 
    private ArrayList<String> damsUoiidsToSync;

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
        damsUoiidsToSync = new ArrayList<>();
        // Grab all the records that have NEVER EVER been synced by CDIS yet
        getNeverSyncedRendition();

        SiAssetMetaData siAsst = new SiAssetMetaData ();        
        
        //Populate column definitions and length array
        siAsst.populateMetaDataDBLengths ();
        
        //Grab all the records that have been synced in the past, but have been updated      
        //sourceUpdatedCDISIdLst = getCISUpdatedRendition();

        if (!damsUoiidsToSync.isEmpty()) {
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
    private void getNeverSyncedRendition() {

        String sqlTypeArr[] = null;
        String sql = null;

        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr =  CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getUnsyncedRecords")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet  rs = pStmt.executeQuery() ) {
            
            logger.log(Level.FINEST,"SQL! " + sql); 
                 
            while (rs.next()) {
                logger.log(Level.ALL, "Adding to list to sync: " + rs.getString(1));
                damsUoiidsToSync.add(rs.getString(1));
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
        for (Iterator<String> iter = damsUoiidsToSync.iterator(); iter.hasNext();) {
            
            CDISMap cdisMap = new CDISMap();

            try {
                
                cdisMap.setDamsUoiid(iter.next());
                cdisMap.populateIdFromUoiid();
                siAsst.setUoiid (cdisMap.getDamsUoiid());
                
                // execute the SQL statment to obtain the metadata and populate variables.  They key value is RenditionID
                mapData(cdisMap);
                
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
                    errorLog.capture(cdisMap, "MSD", "Error, unable to update uois table with new metadata_state_dt " + cdisMap.getDamsUoiid());   
                    continue; 
                }
                
                // Insert row into activity_log table
                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("MDS");
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "CAL",  "Could not create CDIS Activity entry: " + cdisMap.getDamsUoiid());   
                    continue;
                }
                        
            } catch (Exception e) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "MDU", "Error, unable to update Dams with new Metadata " + cdisMap.getDamsUoiid());   
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

    private void mapData(CDISMap cdisMap) {
        String sql;
        String sqlTypeArr[];
        String sqlType;
        String delimiter;
        
        ScrubStringForDb scrub = new ScrubStringForDb();

        logger.log(Level.ALL, "Mapping Data for metadata");
        
        //for each select statement found in the xml 
        for (String key : CDIS.getXmlSelectHash().keySet()) {

            // Get the sql value from the hasharray
            sql = key;
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            sqlType = sqlTypeArr[0];
            delimiter = sqlTypeArr[1];
            
            if (! ((sqlTypeArr[0].equals("singleResult")) || (sqlTypeArr[0].equals("singleResult")))  ){      
                //get the next query, we are not interested in this one at this point
                continue;
            }
                        
            if (sql.contains("?UOI_ID?")) {
                sql = sql.replace("?UOI_ID?", String.valueOf(cdisMap.getDamsUoiid()));
            }
            logger.log(Level.ALL, "select Statement: " + sql);    
            
            try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                 ResultSet rs = pStmt.executeQuery() ) {
                
                // populate the metadata object with the values found from the database query
                this.metaDataValuesForDams = new HashMap <>();
                
                while (rs.next()) {
                    
                    try {
                    
                        ResultSetMetaData rsmd = rs.getMetaData();
                    
                        for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                            String columnName = rsmd.getColumnName(i);
                            String columnValue = rs.getString(i);
                        
                            logger.log(Level.ALL, "columnInfo: " + columnName + " " + columnValue);
                        
                            if (columnValue != null) {
                                scrub.scrubString(columnValue);
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
                        continue;
                    }
                }
            } catch (Exception e) {
                logger.log(Level.ALL, "Error, exception raised in metadata for loop", e); 
                continue;
            } 
        }
    }
}
