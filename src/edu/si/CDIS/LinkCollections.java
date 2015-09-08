
package edu.si.CDIS;


import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.LinkedHashMap;
import edu.si.CDIS.utilties.DataProvider;;
import java.util.logging.Level;
import java.sql.Connection;

import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.CDISActivityLog;

import edu.si.CDIS.DAMS.Database.CDISMap;

  
public class LinkCollections  {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection cisConn;
    Connection damsConn;
    LinkedHashMap <String,String> neverLinkedDamsRendtion;   
    String cisSourceDB;

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
    public void linkToCIS (CDIS cdis) {
        
        // establish connectivity, and other most important variables
        this.damsConn = cdis.damsConn;
        this.cisConn = cdis.cisConn;
        this.cisSourceDB = cdis.properties.getProperty("cisSourceDB"); 
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsRendtion = new LinkedHashMap <String, String>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsRenditions (cdis);
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in the CIS
        linkUANtoFilename (cdis);    
        
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
            logger.log(Level.FINER,"ERROR: Could not update the forDams flag in TMS",e);
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
          
    }
    
    
    /*  Method :        linkUANtoFilename
        Arguments:     
        Description:    Connects the filename in TMS with the DAMS UAN
        RFeldman 4/2015
    */
    private void linkUANtoFilename(CDIS cdis) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;    
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
                CDISMap cdisMap = new CDISMap();
                
                cdisMap.setUoiid(key);
                
                if (sql.contains("?DAMSfileName?")) {
                    currentIterationSql = sql.replace("?DAMSfileName?",  neverLinkedDamsRendtion.get(key));
                }
                
                //logger.log(Level.FINER,"checking for UOI_ID " + cdisTbl.getUOIID() + " UAN: " + neverLinkedDamsRendtion.get(key));
                logger.log(Level.FINEST,"SQL " + currentIterationSql);
                              
                switch (cisSourceDB) {
                        case "CDISDB" :
                             stmt = damsConn.prepareStatement(currentIterationSql);
                             break;
                        case "TMSDB" :
                            stmt = cisConn.prepareStatement(currentIterationSql);
                            break;
                            
                        default:     
                            logger.log(Level.SEVERE, "Error: Invalid ingest source {0}, returning", cisSourceDB );
                            return;
                }
                 
                rs = stmt.executeQuery();              
                        
                if (rs.next()) {
                    
                    try {
                        
                        cdisMap.setCdisMapId(rs.getInt(1));           
                    
                        logger.log(Level.FINER,"Got Linking Pair! UOI_ID! " + cdisMap.getUoiid() + " CDIS_MAP_ID: " + cdisMap.getCdisMapId());
                                                        
                        // update CDISMap table with uoiid
                        boolean uoiidUpdated = cdisMap.updateUoiid(damsConn);
        
                        if (! uoiidUpdated) {
                            logger.log(Level.FINER,"ERROR: CDIS Map record not linked successfully! " + cdisMap.getUoiid());
                            //get the next id from the list
                            continue;
                        }
                        
                        if (cisSourceDB.equals("TMSDB")) {
                            //Update the TMS blob. For TMS only 
                            if (cdis.properties.getProperty("updateTMSThumbnail").equals("true") ) {
                                Thumbnail thumbnail = new Thumbnail();
                                thumbnail.generate (damsConn, cisConn, cdisMap.getUoiid(), Integer.parseInt(cdisMap.getCisUniqueMediaId()));
                            }
                        
                            //This is TMS specific code. For TMS only
                            if (cdis.properties.getProperty("setForDamsFlag").equals("true") ) {
                                setForDamsFlag(Integer.parseInt(cdisMap.getCisUniqueMediaId()));
                            }
                        }
                        
                        //Set the activitylog table for statuscode LC for 'Link Complete'
                        CDISActivityLog activityLog = new CDISActivityLog();
                        boolean activityAdded = activityLog.insertActivity(damsConn, cdisMap.getCdisMapId(), "LC");
                        if (! activityAdded) {
                            logger.log(Level.FINER,"ERROR: Activity not added successfully! " + cdisMap.getUoiid());
                        }
                        
                        //SiAssetMetaData siAsst = new SiAssetMetaData();
                        // we were successful in creating a record in the CDIS Table, we need to update DAMS with the source_system_id
                        // update the SourceSystemID in DAMS with this value
                        //int updatedRows = siAsst.updateDAMSSourceSystemID(damsConn, cdisMap.getUoiid(), "TO BE SYNCED BY CDIS" );
                        
                        //if (updatedRows != 1) {
                        //    logger.log(Level.FINER,"ERROR: Unable to update siAssetMetadata SourceSystemID successfully! " + cdisMap.getUoiid());
                        //}
                        
                    } catch (Exception e) {
                        logger.log(Level.FINER,"ERROR: Catched error in processing for UOIID! " + cdisMap.getUoiid(),e);
                    }
                }
                
            } catch (Exception e) {
                logger.log(Level.FINER,"ERROR: Catched error in setup/Executing of linkUANtoFilename query",e);
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
                addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("NAME"));
                logger.log(Level.FINER,"Adding DAMS asset to lookup in the CIS: {0}", rs.getString("UOI_ID") );
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
