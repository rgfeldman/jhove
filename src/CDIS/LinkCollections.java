
package CDIS;


import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Iterator;
import edu.si.data.DataProvider;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.sql.Connection;

import CDIS.CollectionsSystem.Database.CDISTable;
import CDIS.CollectionsSystem.Database.TMSObject;


        
public class LinkCollections  {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection tmsConn;
    Connection damsConn;
    HashMap <String,String> neverLinkedDamsRendtion;    

    public HashMap <String,String> getNeverLinkedDamsRendtion() {
        return this.neverLinkedDamsRendtion;
    }
       
    private void addNeverLinkedDamsRendtion (String UOIID, String owning_unit_unique_name) {
        this.neverLinkedDamsRendtion.put(UOIID, owning_unit_unique_name); 
    }
    
    /*  Method :       link
        Arguments:      The CDIS object, and the StatisticsReport object
        Description:    link operation specific code starts here
        RFeldman 2/2015
    */
    public void link (CDIS cdis_new, StatisticsReport statReport) {
        
        // establish connectivity, and other most important variables
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
        
        //Populate the header information in the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "link"); 
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsRendtion = new HashMap <String, String>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsRenditions (cdis_new);
        
        statReport.populateStats(neverLinkedDamsRendtion.size(), 0, 0, "link");
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS
        linkUANtoFilename (cdis_new.xmlSelectHash, statReport);    
        
    }
    
    
    /*  Method :        createCDISRecord
        Arguments:      The CDISTbl object, which reflects the CDIS table
        Description:    Inserts a row into the CDIS table
        RFeldman 2/2015
    */
    
    private int updateDAMSSourceSystemID (CDISTable cdisTbl) {
        int recordsUpdated = 0;
        Statement stmt = null;
        
        String sql = "update SI_ASSET_METADATA set source_system_id = '" + cdisTbl.getRenditionNumber() + "' " +
                    "where UOIID = '" + cdisTbl.getUOIID() + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try {
            recordsUpdated = DataProvider.executeUpdate(this.damsConn, sql);
        
            stmt = this.damsConn.createStatement();
            recordsUpdated = stmt.executeUpdate(sql);
        
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
            
        return recordsUpdated;
        
    }
    
    
    private void linkUANtoFilename(HashMap <String,String[]> SelectHash, StatisticsReport statRpt) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;
        String owning_unit_unique_name = null;     
        String newSql = null;
        String sqlTypeArr[] = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : SelectHash.keySet()) {     
              
            if (sqlTypeArr[0].equals("TMSSelect")) {   
                sql = key;    
            }
        }

        //Iterate though hash...the key is the select statement itself
        for (String key : neverLinkedDamsRendtion.keySet()) {

            try {
                CDISTable cdisTbl = new CDISTable();
                
                // set the temporary newSql variable to contain the sql with the UAN from the never linked Rendition hash
                newSql = sql.replace("?owning_unit_unique_name?", neverLinkedDamsRendtion.get(key));
                
                cdisTbl.setUOIID(key);
                
                //logger.log(Level.FINER,"checking for UOI_ID " + cdisTbl.getUOIID() + " UAN: " + neverLinkedDamsRendtion.get(key));
                //logger.log(Level.FINEST,"SQL " + newSql);
                              
                stmt = tmsConn.prepareStatement(newSql);
                rs = stmt.executeQuery();              
                        
                if (rs.next()) {
                    cdisTbl.setRenditionId(rs.getInt(1));
                    cdisTbl.setRenditionNumber(rs.getString(2));           
                    
                    logger.log(Level.FINER,"Got Linking Pair! UOI_ID! " + cdisTbl.getUOIID() + " RenditionID: " + rs.getInt(1));
                    
                    // Get the objectID for the CDIS table by the renditionID if is is ontainable
                    TMSObject tmsObject = new TMSObject();
                    tmsObject.populateObjectIDByRenditionId (cdisTbl.getRenditionId(), tmsConn);                            
                    
                    // Set the objectID in the CDIS table object equal to the ObjectID in the Object object
                    cdisTbl.setObjectId( tmsObject.getObjectID() );
                    
                    // add linking record to CDIS table
                    boolean recordCreated = cdisTbl.createRecord (cdisTbl, tmsConn);
        
                    // If we were successful in creating a record in the CDIS Table, we need to update DAMS with the source_system_id
                    if (recordCreated) {
                        // update the SourceSystemID in DAMS with this value
                        int updatedRows = updateDAMSSourceSystemID(cdisTbl);
                        
                        if (updatedRows == 1) {
                            statRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "link", true);
                        }
                        else if (updatedRows == 0) {
                            statRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "link", false);
                        }
                    }
                    else{
                        logger.log(Level.FINER,"ERROR: CDIS record not created for UOIID! " + cdisTbl.getUOIID());
                        statRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "link", false);
                    }
                    
                }

                
            } catch (Exception e) {
                e.printStackTrace();
        
            }finally {
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
            
        }
        
    }
    
     /*  Method :       updateSourceSystemID
        Arguments:      The CDISTable object
        Returns:        int (number of rows updated)
        Description:    Updates the SI_ASSET_METADATA table, sets source_system_id for a particular UOIID.
                        Both the source_system_id and the UOIID are what is found in the CDISTbl object
        RFeldman 2/2015
    */
    private int updateSourceSystemID (CDISTable cdisTbl) {
        int numRows = 0;
        
        String sql = "update SI_ASSET_METADATA " +
                     "set source_system_id = '" + cdisTbl.getRenditionNumber() + "'" +
                     "where uoi_id = '" + cdisTbl.getUOIID() + "'";
        
        return numRows;
    }
    
    /*  Method :        populateNeverLinkedRenditions
        Arguments:      
        Description:    Populates a hash list that contains DAMS renditions that need to be linked 
                        with the Collection system (TMS)
        RFeldman 2/2015
    */
    private void populateNeverLinkedDamsRenditions (CDIS cdis_new) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String owning_unit_unique_name = null;
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : cdis_new.xmlSelectHash.keySet()) {     
              
            if (sqlTypeArr[0].equals("RetrieveDamsRenditions")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try {
            
            stmt = damsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("OWNING_UNIT_UNIQUE_NAME"));
                logger.log(Level.FINER,"Adding DAMS asset to lookup in TMS: {0}", rs.getString("UOI_ID") );
            }
            
            int numRecords = this.neverLinkedDamsRendtion.size();
            
            logger.log(Level.FINER,"Number of records in DAMS that are unsynced: {0}", numRecords);
            

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return;
        
    }
    
}
