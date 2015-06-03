
package edu.si.CDIS;


import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.LinkedHashMap;
import java.util.Iterator;
import edu.si.CDIS.utilties.DataProvider;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.sql.Connection;

import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;


        
public class LinkCollections  {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection cisConn;
    Connection damsConn;
    LinkedHashMap <String,String> neverLinkedDamsRendtion;    
    int successCount;
    int failCount;

    public LinkedHashMap <String,String> getNeverLinkedDamsRendtion() {
        return this.neverLinkedDamsRendtion;
    }
       
    private void addNeverLinkedDamsRendtion (String UOIID, String owning_unit_unique_name) {
        this.neverLinkedDamsRendtion.put(UOIID, owning_unit_unique_name); 
    }
    
    /*  Method :       linkToCIS
        Arguments:      The CDIS object, and the StatisticsReport object
        Description:    link to CIS operation specific code starts here
        RFeldman 2/2015
    */
    public void linkToCIS (CDIS cdis, StatisticsReport statReport) {
        
        // establish connectivity, and other most important variables
        this.damsConn = cdis.damsConn;
        this.cisConn = cdis.cisConn;
          
        //Populate the header information in the report file
        statReport.populateHeader(cdis.properties.getProperty("siUnit"), "linkToCIS"); 
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsRendtion = new LinkedHashMap <String, String>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsRenditions (cdis);
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS
        linkUANtoFilename (cdis, statReport);    
        
        statReport.populateStats(neverLinkedDamsRendtion.size(), 0, successCount, failCount, "linkToCIS");
        
    }
    
    /*  Method :        setForDamsFlag
        Arguments:      
        Description:    updates the isColor flag...which indicates the rendition is forDAMS
        RFeldman 2/2015
    */
    private void setForDamsFlag(int RenditionId) {
        
        int recordsUpdated = 0;
        Statement stmt = null;
        
        String sql = "update mediaRenditions " +
                    "set IsColor = 1 " +
                    "where IsColor = 0 and RenditionID = " + RenditionId;
        
         logger.log(Level.FINEST, "SQL! {0}", sql);
         
         try {
            recordsUpdated = DataProvider.executeUpdate(this.cisConn, sql);
                   
            logger.log(Level.FINEST,"Rows ForDams flag Updated in CIS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
         
        
    }
    
    private String getFileNameForUoiid (String uoiid) {

        String name = null;
        ResultSet rs = null;
        
        String sql = "select Name "
                + "from UOIS "
                + "where UOI_ID = '" + uoiid + "'";
        
        logger.log(Level.ALL, "Select String: " + sql);

        rs = DataProvider.executeSelect(this.damsConn, sql);
        
        try {
            if (rs.next()) {
                name = rs.getString(1);
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
         
        return name;
    }
    
    /*  Method :        linkUANtoFilename
        Arguments:     
        Description:    Connects the filename in TMS with the DAMS UAN
        RFeldman 4/2015
    */
    private void linkUANtoFilename(CDIS cdis, StatisticsReport statRpt) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;
        String owning_unit_unique_name = null;     
        String currentIterationSql = null;
        String sqlTypeArr[] = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : cdis.xmlSelectHash.keySet()) {     
              
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            
            if (sqlTypeArr[0].equals("checkAgainstCIS")) {   
                sql = key;    
            }
        }

        //Iterate though hash...the key is the select statement itself
        for (String key : neverLinkedDamsRendtion.keySet()) {

            try {
                CDISTable cdisTbl = new CDISTable();
                
                // set the temporary newSql variable to contain the sql with the UAN from the never linked Rendition hash
                if (sql.contains("?owning_unit_unique_name?")) {
                    currentIterationSql = sql.replace("?owning_unit_unique_name?", neverLinkedDamsRendtion.get(key));
                }
                
                cdisTbl.setUOIID(key);
                cdisTbl.setUAN(neverLinkedDamsRendtion.get(key));
                
                if (sql.contains("?DAMSfileName?")) {
                    //get the filename based on the uoiid
                    String fileName = getFileNameForUoiid(cdisTbl.getUOIID());
                    currentIterationSql = sql.replace("?DAMSfileName?", fileName);
                }
                
                //logger.log(Level.FINER,"checking for UOI_ID " + cdisTbl.getUOIID() + " UAN: " + neverLinkedDamsRendtion.get(key));
                logger.log(Level.FINEST,"SQL " + currentIterationSql);
                              
                stmt = cisConn.prepareStatement(currentIterationSql);
                rs = stmt.executeQuery();              
                        
                if (rs.next()) {
                    
                    try {
                        
                        cdisTbl.setRenditionId(rs.getInt(1));
                        cdisTbl.setRenditionNumber(rs.getString(2));           
                    
                        logger.log(Level.FINER,"Got Linking Pair! UOI_ID! " + cdisTbl.getUOIID() + " RenditionID: " + cdisTbl.getRenditionId());
                    
                        // Get the objectID for the CDIS table by the renditionID if is is ontainable
                        TMSObject tmsObject = new TMSObject();
                        tmsObject.populateObjectIDByRenditionId (cdisTbl.getRenditionId(), cisConn);                            
                    
                        // Set the objectID in the CDIS table object equal to the ObjectID in the Object object
                        cdisTbl.setObjectId( tmsObject.getObjectID() );
                    
                        // add linking record to CDIS table
                        boolean recordCreated = cdisTbl.createRecord (cdisTbl, cisConn);
        
                        if (! recordCreated) {
                            logger.log(Level.FINER,"ERROR: CDIS record not created for UOIID! " + cdisTbl.getUOIID());
                            statRpt.writeUpdateStats(cdisTbl.getUAN(), cdisTbl.getRenditionNumber(), "link", false);
                            //get the next id from the list
                            continue;
                        }
                        
                        //Update the TMS blob
                        if (cdis.properties.getProperty("updateTMSThumbnail").equals("true") ) {
                            Thumbnail thumbnail = new Thumbnail();
                            thumbnail.update (damsConn, cisConn, cdisTbl.getUOIID(), cdisTbl.getRenditionId());
                        }
                        
                        if (cdis.properties.getProperty("setForDamsFlag").equals("true") ) {
                            setForDamsFlag(cdisTbl.getRenditionId());
                        }
                        
                        SiAssetMetaData siAsst = new SiAssetMetaData();
                        // we were successful in creating a record in the CDIS Table, we need to update DAMS with the source_system_id
                        // update the SourceSystemID in DAMS with this value
                        int updatedRows = siAsst.updateDAMSSourceSystemID(damsConn, cdisTbl.getUOIID(), cdisTbl.getRenditionNumber() );
                        
                        if (updatedRows == 1) {
                            statRpt.writeUpdateStats(cdisTbl.getUAN(), cdisTbl.getRenditionNumber(), "link", true);
                            successCount ++;
                        }
                        else {
                            statRpt.writeUpdateStats(cdisTbl.getUAN(), cdisTbl.getRenditionNumber(), "link", false);
                            failCount ++;
                        }
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.log(Level.FINER,"ERROR: Catched error in processing for UOIID! " + cdisTbl.getUOIID());
                        statRpt.writeUpdateStats(cdisTbl.getUAN(), cdisTbl.getRenditionNumber(), "link", false);
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
    
    
    /*  Method :        populateNeverLinkedRenditions
        Arguments:      
        Description:    Populates a hash list that contains DAMS renditions that need to be linked 
                        with the Collection system (TMS)
        RFeldman 2/2015
    */
    private void populateNeverLinkedDamsRenditions (CDIS cdis) {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String owning_unit_unique_name = null;
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : cdis.xmlSelectHash.keySet()) {     
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            
            if (sqlTypeArr[0].equals("retrieveDamsImages")) {   
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
            
            logger.log(Level.FINER,"Number of records in DAMS that are unlinked: {0}", numRecords);
            

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return;
        
    }
    
}
