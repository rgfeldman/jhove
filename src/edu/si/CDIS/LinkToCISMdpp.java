/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import java.util.HashMap;
import edu.si.CDIS.Database.CDISMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.SecurityPolicyUois;
import edu.si.CDIS.Database.CdisLinkToCis;

/**
 *
 * @author rfeldman
 */
public class LinkToCISMdpp {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public void link (CDIS cdis) {
        //  The CDIS process will check CDIS for Ingest table for any entries with status RI. 
        
        //from the config file, get the cdis_map_id and the cisId for any records that need to be updated
        String sqlTypeArr[] = null;
        String sql = null;
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        for (String key : cdis.xmlSelectHash.keySet()) {     
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            
            if (sqlTypeArr[0].equals("retrieveImagesToLink")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
        
        HashMap<Integer, String> mapIdsToIntegrate = new HashMap<>();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = cdis.damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs.next()) {
                    logger.log(Level.ALL, "Adding to list to sync: " + rs.getString(1));
                    mapIdsToIntegrate.put(rs.getInt(1), rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        //  Update status and date in CDIS activity log.
        for (Integer key : mapIdsToIntegrate.keySet()) {
            CDISMap cdisMap = new CDISMap();
            CDISActivityLog activityLog = new CDISActivityLog();
            
            cdisMap.setCdisMapId(key);
            cdisMap.setCisUniqueMediaId(mapIdsToIntegrate.get(key));
            
            cdisMap.updateCisUniqueMediaId(cdis.damsConn);
           
            //get the uoi_id (and filename)
            cdisMap.populateMapInfo(cdis.damsConn);
            
            
            //  Update the security policy 
            if (cdisMap.getFileName().endsWith("tif") ) {
                SecurityPolicyUois secPolicy = new SecurityPolicyUois();
                secPolicy.setUoiid(cdisMap.getDamsUoiid());
                
                CdisLinkToCis cdisLinkTbl = new CdisLinkToCis();
                cdisLinkTbl.setCisUniqueMediaId(cdisMap.getCisUniqueMediaId());
                cdisLinkTbl.setSiHoldingUnit(cdis.properties.getProperty("siHoldingUnit"));
                cdisLinkTbl.populateSecPolicyId(cdis.damsConn);
                
                secPolicy.setSecPolicyId(cdisLinkTbl.getSecurityPolicyId());
               
                secPolicy.updateSecPolicyId(cdis.damsConn);          
            }
            
            //  Set public_use = 'Y' 
            SiAssetMetaData siAsst = new SiAssetMetaData();
            siAsst.setUoiid(cdisMap.getDamsUoiid());      
            siAsst.updatePublicUse(cdis.damsConn);
             
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            activityLog.setCdisStatusCd("LCC");
            activityLog.insertActivity(cdis.damsConn);
        
           try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
             
        }

        
    }
    
}
