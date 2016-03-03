/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.sql.ResultSet;

/**
 *
 * @author rfeldman
 */
public class CDISCisMediaType {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    char inCisInd;
    Integer cdisCisMediaTypeId;
    
    public Integer getCdisCisMediaTypeId() {
        return this.cdisCisMediaTypeId;
    }
        
    public char getInCisInd() {
        return this.inCisInd;
    }
    
    public void setCdisCisMediaTypeId (Integer cdisCisMediaTypeId) {
        this.cdisCisMediaTypeId = cdisCisMediaTypeId;
    }
        
    public void setInCis (char inCisInd) {
        this.inCisInd = inCisInd;
    }
    
    public boolean populateInCis () {
  
        String sql = "SELECT in_cis_ind " +
                    "FROM cdis_cis_media_type_id " +
                    "WHERE cdis_cis_media_type_id = " + getCdisCisMediaTypeId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
        
            if (rs != null && rs.next()) {
                setInCis (rs.getString(1).charAt(0));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_map_id from cdis_error_log", e );
                return false;
        }
        return true;
    }
    
}
