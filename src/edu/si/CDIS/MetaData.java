/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.si.CDIS.utilties.DataProvider;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.XmlSqlConfig;
import edu.si.CDIS.utilties.ScrubStringForDb;
import edu.si.CDIS.utilties.TruncateForDB;

public class MetaData {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    String sqlUpdate;
    String siUnit;
    String emailTo;
    String flagForIDS;
    String metaDataXmlFile;
    int NumberOfSyncedRenditions;
    Connection cisConn;
    Connection damsConn;
    int successfulUpdateCount;
    int failedUpdateCount;
    HashMap <String,String> metaDataForDams; 

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
    public void sync(CDIS cdis) {

         //assign the database connections for later use
        this.damsConn = cdis.damsConn;
        this.cisConn = cdis.cisConn;
        
        //obtain properties values from the config file
        this.siUnit = cdis.properties.getProperty("siUnit");
        if (cdis.properties.getProperty("emailTo") != null) {
            this.emailTo = cdis.properties.getProperty("emailTo");
        }
        this.flagForIDS = cdis.properties.getProperty("flagForIDS");

        // initialize renditionID lists for sync
        ArrayList<String> damsNeverSyncedMedia = new ArrayList<String>();
        ArrayList<Integer> sourceUpdatedCDISIdLst = new ArrayList<Integer>();

        // Grab all the records that have NEVER EVER been synced by CDIS yet
        damsNeverSyncedMedia = getNeverSyncedRendition();

        //Grab all the records that have been synced in the past, but have been updated      
        //sourceUpdatedCDISIdLst = getCISUpdatedRendition();

        // Loop through all the rendition IDs we determined we needed to sync
        // while i could have chosen to call the processRendition only one time instead of three, for now
        // I have broken this up to make this more trackable and in case we want to exclude certain types of
        // sync for certain units
        if (!damsNeverSyncedMedia.isEmpty()) {
            processRenditionList(damsNeverSyncedMedia, cdis.xmlSelectHash);
        }
        /*
        if (!sourceUpdatedCDISIdLst.isEmpty()) {
            processRenditionList(sourceUpdatedCDISIdLst, cdis.xmlSelectHash);
        }
        */
    }

    /*  Method :        updateDamsData
        Arguments:      
        Description:    Updates the DAMS with the metadata changes 
        RFeldman 2/2015
    */
    private int updateDamsData() {
        int updateCount = DataProvider.executeUpdate(this.damsConn, getSqlUpdate());

        return updateCount;

    }

    /*  Method :       getNeverSyncedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have never been synced 
        RFeldman 2/2015
    */
    private ArrayList<String> getNeverSyncedRendition() {

        ArrayList<String> damsMediaToSync = new ArrayList<String>();

        // this needs to get the record with the max metadaDataSyncDate if it is not unique
        /*
        String sql = "select CDIS_ID "
                + "from CDIS "
                + "where MetaDataSyncDate is NULL "
                + "order by CDIS_ID";
        */
        String sql = "select uoi_id from cdis_metadata";
        
        logger.log(Level.ALL, "updateStatment: " + sql);

        ResultSet rs;

        //rs = DataProvider.executeSelect(this.cisConn, sql);
        rs = DataProvider.executeSelect(this.damsConn, sql);

        try {
            while (rs.next()) {
                    logger.log(Level.ALL, "Adding to list to sync: " + rs.getString(1));
                    damsMediaToSync.add(rs.getString(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return damsMediaToSync;
    }

    /*  Method :       getCISUpdatedRendition
        Arguments:      
        Description:    get Renditions by CDIS_ID that have been previously synced, 
                        but have been updated in the Collection System (CIS), and DAMS requres those updates 
        RFeldman 2/2015
    */
    /*
    private ArrayList<Integer> getCISUpdatedRendition() {

        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();
        
        // need to check the audit trail.  The objectID in the audittrail table may correspond to the 
        // renditionID, mediaMasterID or the objectID
        
        String sql = "select distinct CDIS_ID " +
                    "from CDIS a, " +
                    "AuditTrail b, " +
                    "MediaRenditions c " +
                    "where a.objectID = b.objectID " +
                    "and a.RenditionId = c.RenditionId " +
                    "and b.tableName = 'Objects' " +
                    "and c.isColor = '1' " +
                    "and b.EnteredDate > a.MetaDataSyncDate " +
                    "union " +
                    "select distinct CDIS_ID " +
                    "from CDIS a, " +
                    "AuditTrail b, " +
                    "MediaRenditions c " +
                    "where a.renditionID = b.objectID " +
                    "and a.RenditionId = c.RenditionId " +
                    "and b.tableName = 'mediaRenditions' " +
                    "and c.isColor = '1' " +
                    "and b.EnteredDate > a.MetaDataSyncDate " +
                    "union " +
                    "select distinct CDIS_ID " +
                    "from CDIS a, " +
                    "AuditTrail b, " + 
                    "MediaRenditions c " +
                    "where c.mediaMasterID = b.ObjectID " +
                    "and a.RenditionId = c.RenditionId " +
                    "and b.tableName = 'mediaMaster' " +
                    "and c.isColor = '1' " +
                    "and b.EnteredDate > a.MetaDataSyncDate ";
                     
        
        ResultSet rs;

        rs = DataProvider.executeSelect(this.cisConn, sql);
        logger.log(Level.ALL, "sql select: " + sql);

        try {
            while (rs.next()) {
                CDISIdList.add(rs.getInt(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return CDISIdList;
    }

    */
    
    
    /*  Method :       processRenditionList
        Arguments:      
        Description:    Goes through the list of rendition Records one at a time 
                        and determines how to update the metadata on each one
        RFeldman 2/2015
    */
    private void processRenditionList(ArrayList<String> imagesToSync, HashMap <String,String[]> SelectHash) {

        // for each Rendition ID that was identified for sync
        for (Iterator<String> iter = imagesToSync.iterator(); iter.hasNext();) {
            
            CDISMap cdisMap = new CDISMap();
            SiAssetMetaData siAsst = new SiAssetMetaData ();

            try {
                
                cdisMap.setUoiid(iter.next());
                
                // execute the SQL statment to obtain the metadata and populate variables.  They key value is RenditionID
                mapData(SelectHash, cdisMap, siAsst);
                
                
                //generate the update statement from the variables obtained in the mapData
                generateUpdate(siAsst, cdisMap);
 
                // Perform the update to the Dams Data
                int updateDamsCount = updateDamsData();
                
                // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
                        if (updateDamsCount == 1) {
                            successfulUpdateCount ++;
                        }
                        
                // Insert row into transaction_log table
                        
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
 
    }
    
    
    /*  Method :        generateUpdate
        Arguments:      
        Returns:      
        Description:    generates the update sql for updating metadata in the DAMS SI_ASSET_METADATA table
        RFeldman 2/2015
    */
    private boolean generateUpdate(SiAssetMetaData siAsst, CDISMap cdisMap) {

        // Go through the list and 
        
        String updateStatement = "UPDATE SI_ASSET_METADATA "
                + " SET source_system_id = '" + siAsst.getSourceSystemId() + "',"
                + " max_ids_size = '" + siAsst.getMaxIdsSize() + "',"
                + " is_restricted = '" + siAsst.getIsRestricted() + "',";
        
        
                
                TruncateForDB trunc = new TruncateForDB();
                                             
                for (String column : metaDataForDams.keySet()) {
            
                        String columnValue = trunc.truncateString(column, metaDataForDams.get(column));
                        if (columnValue.equals("ERROR: UNSUPPORTED COLUMN") ) {
                            return false;
                        }
                        
                        updateStatement = updateStatement + " " + column + " = '" + columnValue + "',";
                     
                }

                updateStatement = updateStatement +
                    " public_use = 'Yes' " +
                    " WHERE uoi_id = '" + cdisMap.getUoiid() + "'";

        //we collected nulls in the set/get commands.  strip out the word null from the update statement
        updateStatement = updateStatement.replaceAll("null", "");

        setSqlUpdate(updateStatement);

        logger.log(Level.ALL, "updateStatment: " + updateStatement);
        
        return true;

    }
    
    /*  Method :        mapData
        Arguments:      
        Returns:      
        Description:    maps the information obtained from the TMS select statement to the appropriate member variables
        RFeldman 2/2015
    */

    private void mapData(HashMap <String,String[]> metaDataSelectHash, CDISMap cdisMap, SiAssetMetaData siAsst) {
        ResultSet rs = null;
        String sql;
        String sqlTypeArr[];
        String sqlType;
        String delimiter;
        PreparedStatement pStmt = null;
        
        ScrubStringForDb scrub = new ScrubStringForDb();

        logger.log(Level.ALL, "Mapping Data for metadata");
        
        //setting defaults
        siAsst.setIsRestricted("Yes");

        for (String key : metaDataSelectHash.keySet()) {

            // Get the sql value from the hasharray
            sql = key;
            sqlTypeArr = metaDataSelectHash.get(key);
            sqlType = sqlTypeArr[0];
            delimiter = sqlTypeArr[1];


            sql = sql.replace("?UOI_ID?", String.valueOf(cdisMap.getUoiid()));
            //sql = sql.replace("?ObjectID?", String.valueOf(cdisTbl.getObjectId()));
            //sql = sql.replace("?RenditionID?", String.valueOf(cdisTbl.getRenditionId()));

            logger.log(Level.ALL, "select Statement: " + sql);
            
            // populate the metadata object with the values found from the database query
            try {
                pStmt = damsConn.prepareStatement(sql);
                rs = pStmt.executeQuery();
            
                this.metaDataForDams = new HashMap <String, String>();
                
                while (rs.next()) {
                    
                    ResultSetMetaData rsmd = rs.getMetaData();
                    
                    for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                        String columnName = rsmd.getColumnName(i);
                        String columnValue = rs.getString(i);
                        
                        logger.log(Level.ALL, "columnInfo: " + columnName + " " + columnValue);
                        
                        scrub.scrubString(columnValue);
                        
                        //check if the column has already been populated
                        String columnExisting = metaDataForDams.get(columnName);
                        if (columnExisting != null) {
                            if (sqlType.equals("cursorAppend")) {
                                //append to existing column
                                metaDataForDams.put(columnName.toLowerCase(), columnExisting + delimiter + ' ' + columnValue);
                            }
                            else {
                                //put out error
                                logger.log(Level.ALL, "Warning: Select statement expected to return single row, returned multiple rows. populating with only one value");
                                metaDataForDams.put(columnName.toLowerCase(),columnValue);
                            }                            
                        }
                        else {
                            //scrub string
   
                            metaDataForDams.put(columnName.toLowerCase(),columnValue);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
           } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
        }
    }

   /*  Method :        updateCDISTbl
        Arguments:      
        Returns:      
        Description:    Update the CDIS table with the new values for when this rendition is re-synced.
        RFeldman 2/2015
    */
    private int updateCDISTbl(CDISTable cdisTbl, String newIDSRestrict) {
        
        String sql = "update CDIS " +
                    "set metaDataSyncDate = SYSDATETIME(), " +
                    "IDSRestrict = '" + newIDSRestrict + "' " +
                    "where CDIS_ID = " + cdisTbl.getCDIS_ID();

        logger.log(Level.ALL, "updateStatment: " + sql);

        int updateCount = DataProvider.executeUpdate(this.cisConn, sql);

        return (updateCount);

    }

    /*  Method :        updateMetaDataStateDate
        Arguments:      
        Returns:      
        Description:    Update the UOIS table with the MetaDataStateDate.  This will trigger IDS.
        RFeldman 2/2015
    */
    private void updateMetaDataStateDate(CDISTable cdisTbl, String newIDSRestrict) {
        int updateCount = 0;
        String previousRestriction = null;
        
        // If the update flag is set to never, we dont have to update for IDS, just return
        if (this.flagForIDS.equals("never")) {
            return;
        }
        // If the restriction has not changed from what is in the database, we may not need to flag for IDS
        else if (this.flagForIDS.equals("default")) {
            previousRestriction = cdisTbl.getIDSRestrict();
            
            if (previousRestriction != null) {
                if (previousRestriction.equals(newIDSRestrict)) {
                    logger.log(Level.ALL, "Flag for IDS not needed");
                    return;  
                }
            }
        }
        // for ifRestricted we send to IDS if the value is Yes or a number
        else if (this.flagForIDS.equals("ifRestricted")) {
            if (! newIDSRestrict.equals("No")) {
 
            }
            else {
                return;
            }
        }
        
        // We have not met any of the above conditions, we should update for IDS
        String sql = "update UOIS set metadata_state_dt = SYSDATE, " +
                    "metadata_state_user_id = '22246' " +
                    "where UOI_ID = '" + cdisTbl.getUOIID() + "'";
        
        logger.log(Level.ALL, "updateUOIIS Statment: " + sql);
        
        DataProvider.executeUpdate(this.damsConn, sql);

    }
    
    /*  Method :        calcNewIDSRestrict
        Arguments:      
        Returns:      
        Description:    calculates the new restriction for insert/compare into the CDIS table
        RFeldman 2/2015
    */
    private String calcNewIDSRestrict (SiAssetMetaData siAsst) {
       
        String IDSRestrict = null;
        
        if (siAsst.getIsRestricted().equals("Yes")) {
            IDSRestrict = "Yes";
        }
        else {
            try {
                int maxIdsSizeInt = Integer.parseInt(siAsst.getMaxIdsSize());
            
                if (maxIdsSizeInt > 0) {
                    IDSRestrict = Integer.toString(maxIdsSizeInt);       
                }
                else {
                    IDSRestrict = "No";
                }
            } catch (Exception e) {
                    IDSRestrict = "No";
            }    
        }
        return IDSRestrict;
    }
}
