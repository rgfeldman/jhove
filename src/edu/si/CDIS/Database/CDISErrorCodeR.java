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
public class CDISErrorCodeR {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String description;
    
    public String getDescription () {
        return this.description;
    }
    
    private void setDescription (String description) {
        this.description = description;
    }
    
    
    public boolean populateDescription (Integer cdisMapId) {
  
        String sql = "SELECT a.description " +
                    "FROM error_code_r a, " +
                    "      cdis_error b " +
                    "WHERE a.error_cd = b.error_cd " +
                    "AND b.cdis_map_id = " + cdisMapId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
        
            if (rs != null && rs.next()) {
                setDescription (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain description from cdis_error_code_r", e );
                return false;
        }
        return true;
    }
}
