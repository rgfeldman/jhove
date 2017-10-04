/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.aaa.database.TblCollection;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class AaaDigitalMediaResource implements CisRecordAttr{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String cisImageIdentifier;
    private String cisGroupIdentifier; 
    
    
    public String getCisImageIdentifier () {
        return this.cisImageIdentifier;
    }
    
    public String getGroupIdentifier () {
        return this.cisGroupIdentifier;
    }
    
    public void setUniqueImageIdentifier (String identifier) {
        this.cisImageIdentifier = identifier;
    }
    
    public boolean populateGroupIdForImageId() {
        
        String sql =    "SELECT fkCollectionID " +
                        "FROM  dbo.tblDigitalMediaResource " +
                        "WHERE digitalMediaResourceID = " + cisImageIdentifier;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                cisGroupIdentifier = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain CollectionID for digResource", e );
                return false;
        }
        return true;
    
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        TblCollection tblCollection = new TblCollection();
        tblCollection.setCollectionId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        tblCollection.populateCollcode();
        
        return "Collection: " + tblCollection.getCollcode();
        
    }
    
    
}
