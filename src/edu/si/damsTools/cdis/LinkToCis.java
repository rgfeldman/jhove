/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.database.CDISActivityLog;
import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.Utils.XmlSqlConfig;

import java.util.logging.Logger;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;

import edu.si.damsTools.DamsTools;


/**
 *
 * @author rfeldman
 */
public class LinkToCis {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public void link () {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        xml.setProjectCd(DamsTools.getProjectCd());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("retrieveImagesToLink"); 
        
        HashMap<Integer, String> mapIdsToIntegrate = new HashMap<>();
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < DamsTools.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }  
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                    logger.log(Level.ALL, "Adding to list to sync: " + rs.getString(1));
                     mapIdsToIntegrate.put(rs.getInt(1), rs.getString(2));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
                return;
            }
        }
        
        //  Update status and date in CDIS activity log.
        for (Integer key : mapIdsToIntegrate.keySet()) {
            CDISMap cdisMap = new CDISMap();
            CDISActivityLog activityLog = new CDISActivityLog();
            
            cdisMap.setCdisMapId(key);
            cdisMap.setCisUniqueMediaId(mapIdsToIntegrate.get(key));
            
            boolean cisIdUpdate = cdisMap.updateCisUniqueMediaId();
            if (!cisIdUpdate) {
                 ErrorLog errorLog = new ErrorLog ();
                 errorLog.capture(cdisMap, "UPCISM", "ERROR: unable to record cis_id in CDIS_MAP table ");
                 continue;
            }
            
            //get the uoi_id (and filename)
            cdisMap.populateMapInfo();
    
            //final validation of all four checksums
            boolean checkSumVldt = validateAllChecksums(cdisMap.getCdisMapId());
            if (! checkSumVldt) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CHKSUM", "ERROR: unable to Validate all checksums with each other");
                continue;
            }
             
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            activityLog.setCdisStatusCd("LCC");
            activityLog.insertActivity();
            
           try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
             
        }
    }
    
    private boolean validateAllChecksums (Integer cdisMapId) {
        
        String sql =    "SELECT 'Vldt Passed' " +
                        "FROM   vfcu_media_file a, " +
                        "       cdis_map b, " +
                        "       towner.checksum_view c, " +
                        "       cdis_link_to_cis d " +
                        "WHERE  a.vfcu_media_file_id = b.vfcu_media_file_id " +
                        "AND    b.dams_uoi_id = c.uoi_id " +
                        "AND    b.cis_unique_media_id  = d.cis_unique_media_id " +
                        "AND    a.vfcu_checksum = a.vendor_checksum " +
                        "AND    a.vendor_checksum = c.content_checksum " +
                        "AND    a.vendor_checksum = d.cis_checksum " +
                        "AND    c.content_checksum = d.cis_checksum " +
                        "AND    b.cdis_map_id = " + cdisMapId;
                
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                return true;
            }   
            else
                return false;
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to validate checksums", e );
                return false;
        }
    }
    
}
