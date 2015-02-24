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
import java.util.Properties;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Iterator;
import CDIS_redesigned.CollectionsSystem.Database.CDISTable;
import edu.si.data.DataProvider;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.logging.Level;


        
public class LinkCollections  {
    
    private final static Logger logger = Logger.getLogger(CDIS_new.class.getName());
    
    Connection tmsConn;
    Connection damsConn;
    String siUnit;
    HashMap <String,String> neverLinkedDamsRendtion;    

    public HashMap <String,String> getNeverLinkedDamsRendtion() {
        return this.neverLinkedDamsRendtion;
    }
       
    private void addNeverLinkedDamsRendtion (String UOIID, String uan) {
        this.neverLinkedDamsRendtion.put(UOIID, uan);
        
        //logger.log(Level.FINER, "Added to list for unsynced renditions UOIID: " + UOIID + " UAN: " + uan);
        
    }
    
    
    public void link (CDIS_new cdis_new) {
        
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
        this.siUnit = cdis_new.properties.getProperty("siUnit");
        
        // read the XML config file and obtain the selectStatements
        xmlConfig xml = new xmlConfig();
               
        xml.read(cdis_new.properties.getProperty("xmlLinkFile"));
                
        
        StatisticsReport statReport = new StatisticsReport();
        
        //System.out.println ("Got XML for linking: " + xml.getSelectStmtHash());
        
        ArrayList<Integer> neverLinkedRenditionLst = new ArrayList<Integer>();
        
        statReport.populateHeader(this.siUnit, "link");
       
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedRenditions (cdis_new.properties.getProperty("uanPrefix"));
        
        statReport.populateStats(neverLinkedRenditionLst.size(), 0, 0, "link");
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS
        linkUANtoFilename (xml.getSelectStmtHash(), statReport);    
        
    }
    
    
    private void populateObjectID (CDISTable cdisTbl) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql =    "select a.ObjectID " +
                        "from Objects a, " +
                        "MediaXrefs b, " +
                        "MediaMaster c, " +
                        "MediaRenditions d " +
                        "where a.ObjectID = b.ID " +
                        "and b.MediaMasterID = c.MediaMasterID " +
                        "and b.TableID = 108 " +
                        "and c.MediaMasterID = d.MediaMasterID " +
                        "and d.RenditionID = " + cdisTbl.getRenditionId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getInt(1) + " For RenditionID: " + cdisTbl.getRenditionId());
                cdisTbl.setObjectId(rs.getInt(1));
            }
        } catch (Exception e) {
                e.printStackTrace();
        
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        
    }
    
    private boolean createCDISRecord(CDISTable cdisTbl) {
        
        boolean inserted = false;
        
        // Get the ObjectID if it exists 

        String sql = "Insert into CDIS (RenditionID, RenditionNumber, ObjectID, UOIID, LinkDate) " +
                    "values ( " + cdisTbl.getRenditionId() + ", '" +
                    cdisTbl.getRenditionNumber() + "', " +
                    cdisTbl.getObjectId() + ", '" +
                    cdisTbl.getUOIID() + "', " +
                    "GETDATE() )";
        
        logger.log(Level.FINEST,"SQL! " + sql);
   
        inserted = DataProvider.executeInsert(this.tmsConn, sql);     
                
        return inserted;
    }
    
    private int updateDAMSSourceSystemID (CDISTable cdisTbl) {
        int recordsUpdated = 0;
        
        String sql = "update SI_ASSET_METADATA set source_system_id = '" + cdisTbl.getRenditionNumber() + "' " +
                    "where UOIID = '" + cdisTbl.getUOIID() + "'";

        logger.log(Level.FINEST,"SQL! " + sql);
        
        return recordsUpdated;
        
    }
    
    
    private void linkUANtoFilename(HashMap <String,String[]> SelectHash, StatisticsReport statRpt) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;
        String UAN = null;      
        
        for (String key : SelectHash.keySet()) {
            sql = key; 
        }
        
        String newSql = null;
        
        //Iterate though hash
        for (String key : neverLinkedDamsRendtion.keySet()) {

            try {
                CDISTable cdisTbl = new CDISTable();
                
                newSql = sql.replace("?UAN?", neverLinkedDamsRendtion.get(key));
                
                cdisTbl.setUOIID(key);
                
                logger.log(Level.FINER,"checking for UOI_ID " + cdisTbl.getUOIID() + " UAN: " + neverLinkedDamsRendtion.get(key));
                logger.log(Level.FINEST,"SQL " + newSql);
                              
                stmt = tmsConn.prepareStatement(newSql);
                rs = stmt.executeQuery();              
                        
                if (rs.next()) {
                    cdisTbl.setRenditionId(rs.getInt(1));
                    cdisTbl.setRenditionNumber(rs.getString(2));           
                    
                    logger.log(Level.FINER,"Got Linking Pair! UOI_ID! " + cdisTbl.getUOIID() + " RenditionID: " + rs.getInt(1));
                    
                    // Get the objectID by the renditionID if is is ontainable
                    populateObjectID (cdisTbl);
                    
                    // add linking record to CDIS table
                    boolean recordCreated = createCDISRecord (cdisTbl);
        
                    if (recordCreated) {
                        // update the SourceSystemID in DAMS with this value
                        int updatedRows = updateDAMSSourceSystemID(cdisTbl);
                        
                        if (updatedRows == 1) {
                            statRpt.writeUpdateStats(cdisTbl, "link", true);
                        }
                        else if (updatedRows == 0) {
                            statRpt.writeUpdateStats(cdisTbl, "link", false);
                        }
                    }
                    else{
                        logger.log(Level.FINER,"ERROR: CDIS record not created for UOIID! " + cdisTbl.getUOIID());
                        statRpt.writeUpdateStats(cdisTbl, "link", false);
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
    
    private int updateSourceSystemID (CDISTable cdisTbl) {
        int numRows = 0;
        
        String sql = "update SI_ASSET_METADATA " +
                     "set source_system_id = '" + cdisTbl.getRenditionNumber() + "'" +
                     "where uoi_id = '" + cdisTbl.getUOIID() + "'";
        
        return numRows;
    }
    
    private void populateNeverLinkedRenditions (String uanPrefix) {
        
        // Get Renditions that have never been linked from DAMS
        String sql = "select UOI_ID, OWNING_UNIT_UNIQUE_NAME " +
                     "from SI_ASSET_METADATA " +
                     "where UPPER(PUBLIC_USE) = 'YES' " +
                     "and SOURCE_SYSTEM_ID IS null " +
                     "and OWNING_UNIT_UNIQUE_NAME like '" + uanPrefix + "%' " +
                     "order by OWNING_UNIT_UNIQUE_NAME ";
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        this.neverLinkedDamsRendtion = new HashMap <String, String>();
        
        try {
           stmt = damsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            while (rs.next()) {
                
                addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("OWNING_UNIT_UNIQUE_NAME"));
                
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
