/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.SecurityPolicyUois;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CdisLinkToCis;
import edu.si.CDIS.utilties.ErrorLog;

import java.util.logging.Logger;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;

/**
 *
 * @author rfeldman
 */
public class LinkToCISMdpp {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public void link () {
        //  The CDIS process will check CDIS for Ingest table for any entries with status RI. 
        
        //from the config file, get the cdis_map_id and the cisId for any records that need to be updated
        String sqlTypeArr[] = null;
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("retrieveImagesToLink")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
        
        HashMap<Integer, String> mapIdsToIntegrate = new HashMap<>();
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
            logger.log(Level.FINEST,"SQL! " + sql); 
            
            while (rs.next()) {
                    logger.log(Level.ALL, "Adding to list to sync: " + rs.getString(1));
                    mapIdsToIntegrate.put(rs.getInt(1), rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
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
                 errorLog.capture(cdisMap, "CCI", "ERROR: unable to record cis_id in CDIS_MAP table ");
                 continue;
            }
            
           
            //get the uoi_id (and filename)
            cdisMap.populateMapInfo();
            
            
            //  Update the security policy 
            if (cdisMap.getFileName().endsWith("tif") ) {
                SecurityPolicyUois secPolicy = new SecurityPolicyUois();
                secPolicy.setUoiid(cdisMap.getDamsUoiid());
                
                CdisLinkToCis cdisLinkTbl = new CdisLinkToCis();
                cdisLinkTbl.setCisUniqueMediaId(cdisMap.getCisUniqueMediaId());
                cdisLinkTbl.setSiHoldingUnit(CDIS.getProperty("siHoldingUnit"));
                boolean secPolicyretrieved = cdisLinkTbl.populateSecPolicyId();
                if (!secPolicyretrieved) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "DSP", "ERROR: unable to Update secuirty Policy in DAMS ");
                    continue;
                }
                
                secPolicy.setSecPolicyId(cdisLinkTbl.getSecurityPolicyId());
               
                boolean secPolicyUpdated = secPolicy.updateSecPolicyId();   
                if (!secPolicyUpdated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "DSP", "ERROR: unable to Update secuirty Policy in DAMS ");
                    continue;
                }
            }
            
            //  Set public_use = 'Y' 
            SiAssetMetaData siAsst = new SiAssetMetaData();
            siAsst.setUoiid(cdisMap.getDamsUoiid());      
            boolean publicUseUpdated = siAsst.updatePublicUse();
            if (! publicUseUpdated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "DPU", "ERROR: unable to Update public use flag in DAMS ");
                continue;
            }
            
             
            activityLog.setCdisMapId(cdisMap.getCdisMapId());
            activityLog.setCdisStatusCd("LCC");
            activityLog.insertActivity();
        
           try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
             
        }

    }
    
}
