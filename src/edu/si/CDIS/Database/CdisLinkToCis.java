/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisLinkToCis {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String cisUniqueMediaId;
    private String siHoldingUnit;
    private Integer securityPolicyId;
    
    
    public String getCisUniqueMediaId () {
        return this.cisUniqueMediaId;
    }
    
    public Integer getSecurityPolicyId () {
        return this.securityPolicyId;
    }
        
    public String getSiHoldingUnit () {
        return this.siHoldingUnit;
    }
        
    
    public void setCisUniqueMediaId (String cisUniqueMediaId) {
        this.cisUniqueMediaId = cisUniqueMediaId;
    }
    
    public void setSecurityPolicyId (Integer securityPolicyId) {
        this.securityPolicyId = securityPolicyId;
    }
    
    public void setSiHoldingUnit (String siHoldingUnit) {
        this.siHoldingUnit = siHoldingUnit;
    }
    
    
    public boolean populateSecPolicyId () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT sec_policy_id " + 
                    "FROM cdis_link_to_cis " +
                    "WHERE cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                    "AND si__holding_unit = '" + getSiHoldingUnit() + "'";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setSecurityPolicyId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return true;
        
    }
    
}
