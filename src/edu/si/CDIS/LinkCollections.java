
package edu.si.CDIS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.LinkedHashMap;
import java.util.logging.Level;

import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISMap;

  
public class LinkCollections  {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    LinkedHashMap <String,String> neverLinkedDamsRendtion;   
    String cisSourceDB;

    public LinkedHashMap <String,String> getNeverLinkedDamsRendtion() {
        return this.neverLinkedDamsRendtion;
    }
       
    private void addNeverLinkedDamsRendtion (String UOIID, String owning_unit_unique_name) {
        this.neverLinkedDamsRendtion.put(UOIID, owning_unit_unique_name); 
    }
    
    /*  Method :       linkToCIS
        Arguments:      The CDIS object
        Description:    link to CIS operation specific code starts here
        RFeldman 2/2015
    */
    public void linkToCIS () {
        
        // establish connectivity, and other most important variables
        this.cisSourceDB = CDIS.getProperty("cisSourceDB"); 
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsRendtion = new LinkedHashMap <String, String>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsMedia ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in the CIS
        linkUANtoFilename ();    
        
    }
    
    /*  Method :        linkUANtoFilename
        Arguments:     
        Description:    Connects the filename in TMS with the DAMS UAN
        RFeldman 4/2015
    */
    private void linkUANtoFilename() {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;    
        String currentIterationSql = null;
        String sqlTypeArr[] = null;
        
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
              
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("checkAgainstCIS")) {   
                sql = key;    
            }
        }

        //Iterate though hash...the key is the select statement itself
        for (String key : neverLinkedDamsRendtion.keySet()) {

            try {
                
                CDISMap cdisMap = new CDISMap();
                
                cdisMap.setDamsUoiid(key);
                
                if (sql.contains("?DAMSfileName?")) {
                    cdisMap.setFileName(neverLinkedDamsRendtion.get(key));
                    currentIterationSql = sql.replace("?DAMSfileName?",cdisMap.getFileName());
                }
                
                logger.log(Level.FINEST,"SQL " + currentIterationSql);
                              
                switch (cisSourceDB) {
                        case "CDISDB" :
                             stmt = CDIS.getDamsConn().prepareStatement(currentIterationSql);
                             break;
                        case "TMSDB" :
                            stmt = CDIS.getCisConn().prepareStatement(currentIterationSql);
                            break;
                            
                        default:     
                            logger.log(Level.SEVERE, "Error: Invalid ingest source {0}, returning", cisSourceDB );
                            return;
                }
                 
                rs = stmt.executeQuery();              
                        
                if (rs.next()) {
                    
                    try {
                        
                        cdisMap.setCdisMapId(rs.getInt(1));    
                        cdisMap.setCisUniqueMediaId(rs.getString(2));
                    
                        logger.log(Level.FINER,"Got Linking Pair! UOI_ID! " + cdisMap.getDamsUoiid() + " CDIS_MAP_ID: " + cdisMap.getCdisMapId());
                                                        
                        // update CDISMap table with uoiid or insert new record.  This can be used to link records that were not sent to ingest 
                        // by CDIS and therefore have no existing mapping record
                        if (CDIS.getProperty("createCdisMapRecord").equals("true")) {
                            // See if a map record exists for this CIS, if it does not then add it
                            cdisMap.createRecord();
                        }
                        else {
                            boolean uoiidUpdated = cdisMap.updateUoiid();
        
                            if (! uoiidUpdated) {
                                logger.log(Level.FINER,"ERROR: CDIS Map record not linked successfully! " + cdisMap.getDamsUoiid());
                                //get the next id from the list
                                continue;
                            }
                        }
                        
                        if (cisSourceDB.equals("TMSDB")) {
                            //Update the TMS blob. For TMS only 
                            if (CDIS.getProperty("updateTMSThumbnail").equals("true") ) {
                                Thumbnail thumbnail = new Thumbnail();
                                thumbnail.generate (cdisMap.getDamsUoiid(), Integer.parseInt(cdisMap.getCisUniqueMediaId()));
                            }
                        
                            //This is TMS specific code. For TMS only
                            if (CDIS.getProperty("setTMSForDamsFlag").equals("true") ) {
                                MediaRenditions mediaRenditions = new MediaRenditions();
                                mediaRenditions.setRenditionId(Integer.parseInt(cdisMap.getCisUniqueMediaId()));
                                mediaRenditions.setForDamsTrue();
                            }
                        }
                        
                        //Set the activitylog table for statuscode LC for 'Link Complete'
                        CDISActivityLog activityLog = new CDISActivityLog();
                        activityLog.setCdisMapId(cdisMap.getCdisMapId());
                        activityLog.setCdisStatusCd("LC");        
                        boolean activityAdded = activityLog.insertActivity();
                        if (! activityAdded) {
                            logger.log(Level.FINER,"ERROR: Activity not added successfully! " + cdisMap.getDamsUoiid());
                        }
                        
                    } catch (Exception e) {
                        logger.log(Level.FINER,"ERROR: Catched error in processing for UOIID! " + cdisMap.getDamsUoiid(),e);
                    }
                    finally {
                        try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); };
                        try { if ( CDIS.getCisConn() != null)  CDIS.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); };
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
    
    
    /*  Method :        populateNeverLinkedMedia
        Arguments:      
        Description:    Populates a hash list that contains DAMS renditions that need to be linked 
                        with the Collection system (TMS)
        RFeldman 2/2015
    */
    private void populateNeverLinkedDamsMedia () {
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("retrieveDamsImages")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try {
            
            stmt = CDIS.getDamsConn().prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("NAME"));
                logger.log(Level.FINER,"Adding DAMS asset to lookup in the CIS: {0}", rs.getString("UOI_ID") );
            }
            
            int numRecords = this.neverLinkedDamsRendtion.size();
            
            logger.log(Level.FINER,"Number of records in DAMS that are unlinked: {0}", numRecords);
            

        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR: Catched error in populateNeverLinkedDamsRenditions",e);
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return;
    }
    
}
