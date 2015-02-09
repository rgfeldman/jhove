/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.HashMap;

import edu.si.data.DataProvider;
import CDIS_redesigned.DAMSDatabase.SiAssetMetaData;
import CDIS_redesigned.SourceDatabase.CDISTable;

public class MetaData {

    /**
     * @param args the command line arguments
     */
    String sqlUpdate;
    String siUnit;
    String emailTo;
    String flagForIDS;
    String metaDataXmlFile;
    int NumberOfSyncedRenditions;
    Connection tmsConn;
    Connection damsConn;

    private void setSqlUpdate(String sqlUpdate) {
        this.sqlUpdate = sqlUpdate;
    }

    private String getSqlUpdate() {
        return this.sqlUpdate;
    }

    // This is the primary method for metadata sync that handles the sync process
    public void sync(Connection tmsConn, Connection damsConn, Properties properties) {

         //assign the database connections for later use
        this.tmsConn = tmsConn;
        this.damsConn = damsConn;
        
        //obtain properties values from the config file
        this.siUnit = properties.getProperty("siUnit");
        if (properties.getProperty("emailTo") != null) {
            this.emailTo = properties.getProperty("emailTo");
        }
        this.flagForIDS = properties.getProperty("flagForIDS");
        this.metaDataXmlFile = properties.getProperty("metaDataXmlFile");
        
        // read the XML config file and obtain the selectStatements
        xmlConfig xml = new xmlConfig();
        xml.read(this.metaDataXmlFile);
        
        StatisticsReport StatReport = new StatisticsReport();
        
         // initialize renditionID lists for sync
        ArrayList<Integer> neverSyncedCDISIdLst = new ArrayList<Integer>();
        ArrayList<Integer> sourceUpdatedCDISIdLst = new ArrayList<Integer>();
        ArrayList<Integer> restrictUpdatedCDISIdLst = new ArrayList<Integer>();

        // Grab all the records that have NEVER EVER been synced by CDIS yet
        neverSyncedCDISIdLst = getNeverSyncedRendition();

        //Grab all the records that have been synced in the past, but have been updated
        sourceUpdatedCDISIdLst = getSourceUpdatedRendition();

        // Grab all the records where the ids or isRestriced flag has been changed.  need to update IDS on these
        restrictUpdatedCDISIdLst = getRestrictUpdatedRenditon();

        StatReport.populateHeader(this.siUnit);
        
        StatReport.populateStats(neverSyncedCDISIdLst.size(), sourceUpdatedCDISIdLst.size(), restrictUpdatedCDISIdLst.size(), "meta");
        
        // create the object to hold the metadata itself
        SiAssetMetaData siAsst = new SiAssetMetaData();

        // Loop through all the rendition IDs we determined we needed to sync
        // while i could have chosen to call the processRendition only one time instead of three, for now
        // I have broken this up to make this more trackable and in case we want to exclude certain types of
        // sync for certain units
        if (!neverSyncedCDISIdLst.isEmpty()) {
            processRenditionList(neverSyncedCDISIdLst, xml, siAsst, StatReport);
        }
        if (!sourceUpdatedCDISIdLst.isEmpty()) {
            processRenditionList(sourceUpdatedCDISIdLst, xml, siAsst, StatReport);
        }
        if (!restrictUpdatedCDISIdLst.isEmpty()) {
            processRenditionList(restrictUpdatedCDISIdLst, xml, siAsst, StatReport);
        }
        
        //sync the imageFilePath.  This essentially should be moved out of metadata sync and be called on its own from the main CDIS
        ImageFilePath imgPath = new ImageFilePath();
        imgPath.sync(tmsConn, damsConn, properties, StatReport);

        //put together the ids sync report
        StatReport.compile();
        
        // Send the report by email if there is an email list
        if (this.emailTo != null) {
            System.out.println("Need to email the report");
            StatReport.send(this.siUnit, this.emailTo);
        }
        
        try {
            damsConn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally { 
        
        }
        
    }

    private int updateDamsData() {
        int updateCount = DataProvider.executeUpdate(this.damsConn, getSqlUpdate());

        return updateCount;

    }


    // get Renditions by CDIS_ID that have never been synced
    private ArrayList<Integer> getNeverSyncedRendition() {

        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();

        // this needs to get the record with the max metadaDataSyncDate
        String sql = "select CDIS_ID "
                + "from CDIS "
                + "where MetaDataSyncDate is NULL "
                + "and RenditionID = 26268";

        System.out.println("updateStatment: " + sql);

        ResultSet rs;

        rs = DataProvider.executeSelect(this.tmsConn, sql);

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

    // get Renditions by CDIS_ID that have been synced before, but have been updated in the source (TMS), and we requre those updates
    // need to populate objectID in the query before this works for us
    private ArrayList<Integer> getSourceUpdatedRendition() {

        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();
        
        String sql = "select CDIS_ID " +
                     "from CDIS a, " +
                     "AuditTrail b, " +
                     "MediaRenditions c " +
                     "where a.objectID = b.objectID " +
                     "and a.RenditionId = c.RenditionId " +
                     "and c.isColor = '1' " +
                     "and b.EnteredDate > a.MetaDataSyncDate";
        
        ResultSet rs;

        rs = DataProvider.executeSelect(this.tmsConn, sql);

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

    private ArrayList<Integer> getRestrictUpdatedRenditon() {

        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();
        
        ResultSet rs;
        
        /* this is currently not coded.  the idea behind it is to get changes in the restriction status 
        this may take an extensive join, not sure if we need to implement this */
        
        return CDISIdList;
    }

    private void processRenditionList(ArrayList<Integer> CDISIdList, xmlConfig xml, SiAssetMetaData siAsst, 
                                StatisticsReport statRpt) {

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

                    // store the current RenditionID in the object, and send to sql statement
                    cdisTbl.setCDIS_ID(iter.next());
                    stmt.setInt(1, cdisTbl.getCDIS_ID());

                    rs = DataProvider.executeSelect(this.tmsConn, stmt);

                    System.out.println("Getting information for CDIS_ID: " + cdisTbl.getCDIS_ID());

                    // Get the supplemental information for the current RenditionID that we are looping through
                    if (rs.next()) {
                        cdisTbl.setRenditionId(rs.getInt("RenditionID"));
                        cdisTbl.setRenditionNumber(rs.getString("RenditionNumber"));
                        cdisTbl.setUOIID(rs.getString("UOIID"));
                        cdisTbl.setMetaDataSyncDate(rs.getString("MetaDataSyncDate"));
                        cdisTbl.setIDSRestrict(rs.getString("IDSRestrict"));
                        cdisTbl.setObjectId(rs.getInt("objectID"));

                        // execute the SQL statment to obtain the metadata.  They key value is RenditionID
                        mapData(xml.getSelectStmtHash(), cdisTbl, siAsst);

                        generateUpdate(siAsst, cdisTbl);

                        // Perform the update to the Dams Data
                        int updateDamsCount = updateDamsData();
                     
                        // If we successfully updated the metadata table in DAMS, record the transaction in the log table, and flag for IDS
                        if (updateDamsCount > 0) {
                            
                            if (updateDamsCount > 1) {
                                System.out.println("unexpected condition, updated more than a single row in dams for uoiid: " + cdisTbl.getUOIID());
                            }
                            
                            statRpt.writeUpdateStats(cdisTbl, "metaData", true);
                           
                            //calcute the new IDSRestriction value
                            String newIDSRestrict = calcNewIDSRestrict(siAsst);
                            
                            // if the flag from the config file says we never update for IDS, skip this step.
                            if (! this.flagForIDS.equals("never")) {
                                updateMetaDataStateDate(cdisTbl, newIDSRestrict);
                            }
                            updateCDISTbl(cdisTbl, newIDSRestrict);
                        }
                        else {
                            statRpt.writeUpdateStats(cdisTbl, "metaData", false);
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
    
    
    // generate the update statement which will update the DAMS database
    private void generateUpdate(SiAssetMetaData siAsst, CDISTable cdisTbl) {

        String updateStatement = "UPDATE SI_ASSET_METADATA "
                + " SET credit = '" + siAsst.getCredit() + "',"
                + " caption = '" + siAsst.getCaption() + "',"
                + " digital_item_notes = '" + siAsst.getDigitalItemNotes() + "',"
                + " description = '" + siAsst.getDescription() + "',"
                + " group_title = '" + siAsst.getGroupTitle() + "',"
                + " is_restricted = '" + siAsst.getIsRestricted() + "',"
                + " keywords = '" + siAsst.getKeywords() + "',"
                + " max_ids_size = '" + siAsst.getMaxIdsSize() + "',"
                + " other_constraints = '" + siAsst.getOtherConstraints() + "',"
                + " primary_creator = '" + siAsst.getPrimaryCreator() + "',"
                + " series_title = '" + siAsst.getSeriesTitle() + "',"
                + " source_system_Id = '" + siAsst.getSourceSystemId() + "',"
                + " terms_and_restrictions = '" + siAsst.getTermsAndRestrictions() + "',"
                + " title = '" + siAsst.getTitle() + "',"
                + " use_restrictions = '" + siAsst.getUseRestrictions() + "',"
                + " work_creation_date = '" + siAsst.getWorkCreationDate() + "',"
                + " public_use = 'Yes' "
                + " WHERE uoi_id = '" + cdisTbl.getUOIID() + "'";

        //we collected nulls in the set/get commands.  strip out the word null from the update statement
        updateStatement = updateStatement.replaceAll("null", "");

        setSqlUpdate(updateStatement);

        System.out.println("updateStatment: " + updateStatement);

    }

    private void mapData(HashMap <String,String[]> metaDataSelectHash, CDISTable cdisTbl, SiAssetMetaData siAsst) {
        ResultSet rs;
        String sql;
        String sqlTypeArr[];
        String sqlType;
        String delimiter;

        System.out.println("Mapping Data for metadata");
        
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

            System.out.println("select Statement: " + sql);

            rs = DataProvider.executeSelect(this.tmsConn, sql);

            // populate the metadata object with the values found from the database query
            try {
                while (rs.next()) {
                    selectCount ++;
                    
                    if (selectCount > 1) {
                        if (sqlType.equals("singleResult")) {
                            System.out.println("Warning: Select statement expected to return single row, returned multiple rows");
                        }
                    }
                    
                    if (sql.contains("AS caption")) {
                        if (rs.getString("caption") != null) {
                            siAsst.setCaption(rs.getString("caption"));
                        }
                    }
                    if (sql.contains("AS credit")) {
                        if (rs.getString("credit") != null) {
                            siAsst.setCredit(rs.getString("credit"));
                        }
                    }
                    if (sql.contains("AS digital_item_notes")) {
                        if (rs.getString("digital_item_notes") != null) {
                            siAsst.setDigitalItemNotes(rs.getString("digital_item_notes"));
                        }
                    }
                    if (sql.contains("AS description")) {
                        if (rs.getString("description") != null) {
                            siAsst.setDescription(rs.getString("description"));
                        }
                    }
                    if (sql.contains("AS group_title")) {
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
                        if (rs.getString("other_constraints") != null) {
                            siAsst.setOtherConstraints(rs.getString("other_constraints"));
                        }
                    }
                    if (sql.contains("AS primary_creator")) {
                        if (rs.getString("primary_creator") != null) {
                            siAsst.setPrimaryCreator(rs.getString("primary_creator"));
                        }
                    }
                    if (sql.contains("AS series_title")) {
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
                        if (rs.getString("terms_and_restrictions") != null) {
                            siAsst.setTermsAndRestrictions(rs.getString("terms_and_restrictions"));
                        }
                    }
                    if (sql.contains("AS title")) {
                        if (rs.getString("title") != null) {
                            siAsst.setTitle(rs.getString("title"));
                        }
                    }
                    if (sql.contains("AS use_restrictions")) {
                        if (rs.getString("use_restrictions") != null) {
                            siAsst.setUseRestrictions(rs.getString("use_restrictions"));
                        }
                    }
                    if (sql.contains("AS work_creation_date")) {
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

    // Update CDIS table to log this transaction
    private int updateCDISTbl(CDISTable cdisTbl, String newIDSRestrict) {
        
        String sql = "update CDIS " +
                    "set metaDataSyncDate = SYSDATETIME(), " +
                    "IDSRestrict = '" + newIDSRestrict + "' " +
                    "where CDIS_ID = " + cdisTbl.getCDIS_ID();

        System.out.println("updateStatment: " + sql);

        int updateCount = DataProvider.executeUpdate(this.tmsConn, sql);

        return (updateCount);

    }

    // Update the UOIS table with the MetaDataStateDate.  This will trigger IDS.
    // We only want to trigger IDS in certain conditions...this is configurable in the config file.
    private int updateMetaDataStateDate(CDISTable cdisTbl, String newIDSRestrict) {
        int updateCount = 0;

        // If the restriction has not changed from what is in the database, we may not need to flag for IDS
        if (this.flagForIDS.equals("ifRestrictUpdated")) {
            if (cdisTbl.getIDSRestrict().equals(newIDSRestrict)) {
                System.out.println("Flag for IDS not needed");
                return 0;
            }
        }
        
        String sql = "update UOIS set metadata_state_dt = SYSDATE " +
                    "where UOI_ID = '" + cdisTbl.getUOIID() + "'";
        
        System.out.println("updateUOIIS Statment: " + sql);
        //updateCount = DataProvider.executeUpdate(this.damsConn, sql);
        
        return (updateCount);

    }
    
     // Get the new restrictions.  We record these in case there are any restriction changes later because restriction changes trigger IDS
    private String calcNewIDSRestrict (SiAssetMetaData siAsst) {
       
        String IDSRestrict;
        
        if (siAsst.getIsRestricted().equals("Yes")) {
            IDSRestrict = "Yes";
        }
        else {
            int maxIdsSizeInt = Integer.parseInt(siAsst.getMaxIdsSize());
            
            if (maxIdsSizeInt > 0) {
                IDSRestrict = Integer.toString(maxIdsSizeInt);       
            }
            else {
                IDSRestrict = "No";
            }
        }
        return IDSRestrict;
    }
}
