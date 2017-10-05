/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdisutilities.ErrorLog;

import java.util.logging.Logger;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.ArrayList;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlQueryData;


/**
 *
 * @author rfeldman
 */
public class LinkToCis extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
            
    public LinkToCis() {
    }
    
    
    public void invoke () {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveImagesToLink");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return ;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        HashMap<Integer, String> mapIdsToIntegrate = new HashMap<>();
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
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
        
        //  Update status and date in CDIS activity log.
        for (Integer key : mapIdsToIntegrate.keySet()) {
            CdisMap cdisMap = new CdisMap();
            CdisActivityLog activityLog = new CdisActivityLog();
            
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
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
    
}
