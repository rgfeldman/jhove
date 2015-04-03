/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CIS.ImageFilePath;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.si.CDIS.utilties.DataProvider;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.StatisticsReport;
import edu.si.CDIS.XmlSqlConfig;

public class MetaData {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    String sqlUpdate;
    String siUnit;
    String emailTo;
    String flagForIDS;
    String metaDataXmlFile;
    int NumberOfSyncedRenditions;
    Connection tmsConn;
    Connection damsConn;
    int successfulUpdateCount;
    ArrayList<String> columnsToUpdate;

    private void setSqlUpdate(String sqlUpdate) {
        this.sqlUpdate = sqlUpdate;
    }

    private String getSqlUpdate() {
        return this.sqlUpdate;
    }

    // This is the primary method for metadata sync that handles the sync process
    public void sync(CDIS cdis_new, StatisticsReport statReport) {

         //assign the database connections for later use
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
        
        //obtain properties values from the config file
        this.siUnit = cdis_new.properties.getProperty("siUnit");
        if (cdis_new.properties.getProperty("emailTo") != null) {
            this.emailTo = cdis_new.properties.getProperty("emailTo");
        }
        this.flagForIDS = cdis_new.properties.getProperty("flagForIDS");
               
         // initialize renditionID lists for sync
        ArrayList<Integer> neverSyncedCDISIdLst = new ArrayList<Integer>();
        ArrayList<Integer> sourceUpdatedCDISIdLst = new ArrayList<Integer>();

        // Grab all the records that have NEVER EVER been synced by CDIS yet
        neverSyncedCDISIdLst = getNeverSyncedRendition();

        //Grab all the records that have been synced in the past, but have been updated
        sourceUpdatedCDISIdLst = getCISUpdatedRendition();

        statReport.populateHeader(this.siUnit, "sync");

        // Loop through all the rendition IDs we determined we needed to sync
        // while i could have chosen to call the processRendition only one time instead of three, for now
        // I have broken this up to make this more trackable and in case we want to exclude certain types of
        // sync for certain units
        if (!neverSyncedCDISIdLst.isEmpty()) {
            processRenditionList(neverSyncedCDISIdLst, cdis_new.xmlSelectHash, statReport);
        }
        if (!sourceUpdatedCDISIdLst.isEmpty()) {
            processRenditionList(sourceUpdatedCDISIdLst, cdis_new.xmlSelectHash, statReport);
        }
        statReport.populateStats(neverSyncedCDISIdLst.size(), sourceUpdatedCDISIdLst.size(), successfulUpdateCount, "meta");
    }

    private int updateDamsData() {
        int updateCount = DataProvider.executeUpdate(this.damsConn, getSqlUpdate());

        return updateCount;

    }


    // get Renditions by CDIS_ID that have never been synced
    private ArrayList<Integer> getNeverSyncedRendition() {

        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();

        // this needs to get the record with the max metadaDataSyncDate if it is not unique
        String sql = "select CDIS_ID "
                + "from CDIS "
                + "where MetaDataSyncDate is NULL "
                + "order by CDIS_ID";
        
        logger.log(Level.ALL, "updateStatment: " + sql);

        ResultSet rs;

        rs = DataProvider.executeSelect(this.tmsConn, sql);

        try {
            while (rs.next()) {
                    logger.log(Level.ALL, "Adding to list to sync: " + rs.getInt(1));
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

    // get Renditions by CDIS_ID that have been previously synced, but have been updated in the Collection System (CIS), and we requre those updates
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

        rs = DataProvider.executeSelect(this.tmsConn, sql);
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

    private void processRenditionList(ArrayList<Integer> CDISIdList, HashMap <String,String[]> SelectHash, StatisticsReport statRpt) {

        // For each Rendition Number in list, obtain information from CDIS table
        PreparedStatement stmt = null;

        try {
            // prepare the sql for obtaining info from CDIS
            stmt = this.tmsConn.prepareStatement("select RenditionID, RenditionNumber, UOIID, MetaDataSyncDate, IDSRestrict, ObjectID "
                    + "from CDIS "
                    + "where CDIS_ID = ? "
                    + "order by CDIS_ID");
        } catch (Exception e) {
            e.printStackTrace();
        }

        ResultSet rs = null;

        try {

            // for each Rendition ID that was identified for sync
            for (Iterator<Integer> iter = CDISIdList.iterator(); iter.hasNext();) {

                try {

                    // Reassign object with each loop
                    CDISTable cdisTbl = new CDISTable();
                    
                    // create the object to hold the metadata itself
                    SiAssetMetaData siAsst = new SiAssetMetaData();

                    // store the current RenditionID in the object, and send to sql statement
                    cdisTbl.setCDIS_ID(iter.next());
                    stmt.setInt(1, cdisTbl.getCDIS_ID());

                    rs = DataProvider.executeSelect(this.tmsConn, stmt);

                    logger.log(Level.ALL, "Getting information for CDIS_ID: " + cdisTbl.getCDIS_ID());

                    // Get the supplemental information for the current RenditionID that we are looping through
                    if (rs.next()) {
                        cdisTbl.setRenditionId(rs.getInt("RenditionID"));
                        cdisTbl.setRenditionNumber(rs.getString("RenditionNumber"));
                        cdisTbl.setUOIID(rs.getString("UOIID"));
                        cdisTbl.setMetaDataSyncDate(rs.getString("MetaDataSyncDate"));
                        cdisTbl.setIDSRestrict(rs.getString("IDSRestrict"));
                        cdisTbl.setObjectId(rs.getInt("objectID"));

                        // execute the SQL statment to obtain the metadata.  They key value is RenditionID
                        mapData(SelectHash, cdisTbl, siAsst);

                        generateUpdate(siAsst, cdisTbl);

                        // Perform the update to the Dams Data
                        int updateDamsCount = updateDamsData();
                     
                         logger.log(Level.FINER, "DAMS Rows updated: " + updateDamsCount );
                        
                        // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
                        if (updateDamsCount == 1) {

                            statRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "metaData", true);
                            successfulUpdateCount ++;
                           
                            //calcute the new IDSRestriction value
                            String newIDSRestrict = calcNewIDSRestrict(siAsst);
                            
                            // if the flag from the config file says we never update for IDS, skip this step.
                            if (! this.flagForIDS.equals("never")) {
                                updateMetaDataStateDate(cdisTbl, newIDSRestrict);
                            }
                            
                            logger.log(Level.ALL, "About to update CDIS table");
                            
                            int cdisRecordsUpdated = updateCDISTbl(cdisTbl, newIDSRestrict);
                            if (cdisRecordsUpdated != 1) {
                                logger.log(Level.ALL, "Error, CDIS Table not updated");
                            }
                        }
                        else {
                            logger.log(Level.ALL, "Error, CDIS Table not updated, metadata not synced");
                            statRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "metaData", false);
                            
                        }
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

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
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
    private void generateUpdate(SiAssetMetaData siAsst, CDISTable cdisTbl) {

        // Go through the list and 
        
        String updateStatement = "UPDATE SI_ASSET_METADATA "
                + " SET source_system_id = '" + siAsst.getSourceSystemId() + "',"
                + " max_ids_size = '" + siAsst.getMaxIdsSize() + "',"
                + " is_restricted = '" + siAsst.getIsRestricted() + "',";
        
                if(!columnsToUpdate.isEmpty()) {
                    
                    for(Iterator<String> iter = columnsToUpdate.iterator(); iter.hasNext();) {
                        String column = iter.next();
                         
                        if (column.equals("credit")) {
                            updateStatement = updateStatement + " credit = '" + siAsst.getCredit() + "',";
                        }
                        else if (column.equals("caption")) {
                            updateStatement = updateStatement + " caption = '" + siAsst.getCaption() + "',";
                        }
                        else if (column.equals("digital_item_notes")) {
                            updateStatement = updateStatement + " digital_item_notes = '" + siAsst.getDigitalItemNotes() + "',";
                        }
                        else if (column.equals("description")) {
                            updateStatement = updateStatement + " description = '" + siAsst.getDescription() + "',";
                        }
                        else if (column.equals("group_title")) {
                            updateStatement = updateStatement + " group_title = '" + siAsst.getGroupTitle() + "',";
                        }
                        else if (column.equals("keywords")) {
                            updateStatement = updateStatement + " keywords = '" + siAsst.getKeywords() + "',";
                        }
                        else if (column.equals("other_constraints")) {
                            updateStatement = updateStatement + " other_constraints = '" + siAsst.getOtherConstraints() + "',";
                        }
                        else if (column.equals("primary_creator")) {
                            updateStatement = updateStatement + " primary_creator = '" + siAsst.getPrimaryCreator() + "',";
                        }
                        else if (column.equals("rights_holder")) {
                            updateStatement = updateStatement + " rights_holder = '" + siAsst.getRightsHolder() + "',";
                        }
                        else if (column.equals("series_title")) {
                             updateStatement = updateStatement + " series_title = '" + siAsst.getSeriesTitle() + "',";
                        }
                        else if (column.equals("terms_and_restrictions")) {
                            updateStatement = updateStatement + " terms_and_restrictions = '" + siAsst.getTermsAndRestrictions() + "',";
                        }
                        else if (column.equals("title")) {
                            updateStatement = updateStatement + " title = '" + siAsst.getTitle() + "',";
                        }
                        else if (column.equals("use_restrictions")) {
                            updateStatement = updateStatement + " use_restrictions = '" + siAsst.getUseRestrictions() + "',";
                        }
                        else if (column.equals("work_creation_date")) {
                            updateStatement = updateStatement + " work_creation_date = '" + siAsst.getWorkCreationDate() + "',";
                        }
                    }
                }

                updateStatement = updateStatement +
                    " public_use = 'Yes' " +
                    " WHERE uoi_id = '" + cdisTbl.getUOIID() + "'";

        //we collected nulls in the set/get commands.  strip out the word null from the update statement
        updateStatement = updateStatement.replaceAll("null", "");

        setSqlUpdate(updateStatement);

        logger.log(Level.ALL, "updateStatment: " + updateStatement);

    }

    /*  Method :        mapData
        Arguments:      
        Returns:      
        Description:    maps the information obtained from the TMS select statement to the appropriate member variables
        RFeldman 2/2015
    */
    private void mapData(HashMap <String,String[]> metaDataSelectHash, CDISTable cdisTbl, SiAssetMetaData siAsst) {
        ResultSet rs;
        String sql;
        String sqlTypeArr[];
        String sqlType;
        String delimiter;
        
        this.columnsToUpdate = new ArrayList<String>();

        logger.log(Level.ALL, "Mapping Data for metadata");
        
        //setting defaults
         siAsst.setIsRestricted("Yes");
         siAsst.setMaxIdsSize("0");

        //for (Iterator<String> iter = metaDataSelect.iterator(); iter.hasNext();) {
        //for (Iterator<String> iter = metaDataSelectHash.iterator(); iter.hasNext();) {
        for (String key : metaDataSelectHash.keySet()) {

            // Get the sql value from the hasharray
            sql = key;
            sqlTypeArr = metaDataSelectHash.get(key);
            sqlType = sqlTypeArr[0];
            delimiter = sqlTypeArr[1];
            
            int selectCount = 0;

            sql = sql.replace("?RenditionID?", String.valueOf(cdisTbl.getRenditionId()));
            sql = sql.replace("?ObjectID?", String.valueOf(cdisTbl.getObjectId()));

            logger.log(Level.ALL, "select Statement: " + sql);

            rs = DataProvider.executeSelect(this.tmsConn, sql);

            // populate the metadata object with the values found from the database query
            try {
                while (rs.next()) {
                    selectCount ++;
                    
                    if (selectCount > 1) {
                        if (sqlType.equals("singleResult")) {
                            logger.log(Level.ALL, "Warning: Select statement expected to return single row, returned multiple rows");
                        }
                    }
                    
                    if (sql.contains("AS caption")) {
                        columnsToUpdate.add("caption");
                        if (rs.getString("caption") != null) {
                            siAsst.setCaption(rs.getString("caption"));
                        }
                    }
                    if (sql.contains("AS credit")) {
                        columnsToUpdate.add("credit");
                        if (rs.getString("credit") != null) {
                            siAsst.setCredit(rs.getString("credit"));
                        }
                    }
                    if (sql.contains("AS digital_item_notes")) {
                        columnsToUpdate.add("digital_item_notes");
                        if (rs.getString("digital_item_notes") != null) {
                            siAsst.setDigitalItemNotes(rs.getString("digital_item_notes"));
                        }
                    }
                    if (sql.contains("AS description")) {
                        columnsToUpdate.add("description");
                        if (rs.getString("description") != null) {
                            siAsst.setDescription(rs.getString("description"));
                        }
                    }
                    if (sql.contains("AS group_title")) {
                        columnsToUpdate.add("group_title");
                        if (rs.getString("group_title") != null) {
                            siAsst.setGroupTitle(rs.getString("group_title"));
                        }
                    }
                    if (sql.contains("AS is_restricted")) {
                        if (rs.getString("is_restricted") != null) {
                            siAsst.setIsRestricted(rs.getString("is_restricted"));
                        }
                    }
                    if (sql.contains("AS keywords")) {
                        columnsToUpdate.add("keywords");
                        if (rs.getString("keywords") != null) {
                            if (sqlType.equals("cursorAppend")) {
                                siAsst.appendKeywords(rs.getString("keywords"),delimiter);
                            }
                            else {
                                siAsst.setKeywords(rs.getString("keywords"));
                            }
                        }
                    }
                    if (sql.contains("AS max_ids_size")) {
                        if (rs.getString("max_ids_size") != null) {
                            siAsst.setMaxIdsSize(rs.getString("max_ids_size"));
                        }
                    }
                    if (sql.contains("AS other_constraints")) {
                        columnsToUpdate.add("other_constraints");
                        if (rs.getString("other_constraints") != null) {
                            siAsst.setOtherConstraints(rs.getString("other_constraints"));
                        }
                    }
                    if (sql.contains("AS primary_creator")) {
                        columnsToUpdate.add("primary_creator");
                        if (rs.getString("primary_creator") != null) {
                            siAsst.setPrimaryCreator(rs.getString("primary_creator"));
                        }
                    }
                    if (sql.contains("AS rights_holder")) {
                        columnsToUpdate.add("rights_holder");
                        if (rs.getString("rights_holder") != null) {
                            siAsst.setRightsHolder(rs.getString("rights_holder"));
                        }
                    }
                    if (sql.contains("AS series_title")) {
                        columnsToUpdate.add("series_title");
                        if (rs.getString("series_title") != null) {
                            siAsst.setSeriesTitle(rs.getString("series_title"));
                        }
                    }
                    if (sql.contains("AS source_system_id")) {
                        if (rs.getString("source_system_id") != null) {
                            siAsst.setSourceSystemId(rs.getString("source_system_id"));
                        }
                    }
                    if (sql.contains("AS terms_and_restrictions")) {
                        columnsToUpdate.add("terms_and_restrictions");
                        if (rs.getString("terms_and_restrictions") != null) {
                            siAsst.setTermsAndRestrictions(rs.getString("terms_and_restrictions"));
                        }
                    }
                    if (sql.contains("AS title")) {
                        columnsToUpdate.add("title");
                        if (rs.getString("title") != null) {
                            siAsst.setTitle(rs.getString("title"));
                        }
                    }
                    if (sql.contains("AS use_restrictions")) {
                        columnsToUpdate.add("use_restrictions");
                        if (rs.getString("use_restrictions") != null) {
                            siAsst.setUseRestrictions(rs.getString("use_restrictions"));
                        }
                    }
                    if (sql.contains("AS work_creation_date")) {
                        columnsToUpdate.add("work_creation_date");
                        if (rs.getString("work_creation_date") != null) {
                            siAsst.setWorkCreationDate(rs.getString("work_creation_date"));
                        }
                    }
                }
            } catch (SQLException e) {
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

        int updateCount = DataProvider.executeUpdate(this.tmsConn, sql);

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
