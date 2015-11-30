/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;


/**
 *
 * @author rfeldman
 */
public class SecurityPolicyUois {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
     
    private String uoiid;
    private Integer secPolicyId;
    
    
    
    public Integer getSecPolicyId() {
           return this.secPolicyId;
    }
    
    public String getUoiid() {
           return this.uoiid;
    }
    
    public void setSecPolicyId (Integer secPolicyId) {
        this.secPolicyId = secPolicyId;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
        
     
     
     public boolean updateSecPolicyId(Connection damsConn) {
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET sec_policy_id = " + getSecPolicyId() +
                      " WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update security policy table with new security policy", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }   
        
        return true;
    }
    
}
