/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisLinkToCis {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String cisUniqueMediaId;
    private Integer securityPolicyId;
    
    
    public String getCisUniqueMediaId () {
        return this.cisUniqueMediaId;
    }
    
    public Integer getSecurityPolicyId () {
        return this.securityPolicyId;
    }
        
    
    public void setCisUniqueMediaId (String cisUniqueMediaId) {
        this.cisUniqueMediaId = cisUniqueMediaId;
    }
    
    public void setSecurityPolicyId (Integer securityPolicyId) {
        this.securityPolicyId = securityPolicyId;
    }
    
    public boolean populateSecPolicyId () {
        
        String sql = "SELECT sec_policy_id " + 
                    "FROM cdis_link_to_cis " +
                    "WHERE cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                    "AND si_holding_unit = '" + CDIS.getSiHoldingUnit() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ){
 
            if (rs != null && rs.next()) {
                setSecurityPolicyId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
}
