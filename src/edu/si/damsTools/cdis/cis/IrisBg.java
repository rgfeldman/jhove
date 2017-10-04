/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.iris.database.SI_IrisDAMSMetaCore;
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
public class IrisBg implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String cisImageIdentifier;
    private String cisGroupIdentifier; 
    
    public void setUniqueImageIdentifier (String identifier) {
        this.cisImageIdentifier = identifier;
    }
    
    public String getCisImageIdentifier () {
        return this.cisImageIdentifier;
    }
    
    public String getGroupIdentifier () {
        return this.cisGroupIdentifier;
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        return "Accno: " + cdisObjectMap.getCisUniqueObjectId();    
    }
    
    public boolean populateGroupIdForImageId() {
        
         //get earliest objectId on the current renditionID       
        String sql =    "SELECT itemAccnoFull " +
                        "FROM  SI_IrisDAMSMetaCore5 " +
                        "WHERE ImageLibId = '" + this.cisImageIdentifier + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                cisGroupIdentifier = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain itemAccnoFull for ImageLibId", e );
                return false;
        }
        return true;
    }
        
}
