/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class SiRestrictionsDtls {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    String uoiId;
    String restrictions;
    
    public String getUoiId() {
        return this.uoiId;
    }
    
    public String getRestrictions() {
        return this.restrictions;
    }
    
    
    public void setRestrictions (String restrictions) {
        this.restrictions = restrictions;
    }
        
    public void setUoiId (String uoiId) {
        this.uoiId = uoiId;
    }
    
    //update the restrictions, set the restriction to the existing restriction
    public boolean deleteRestrictions() {
        String sql = "DELETE FROM towner.si_restrictions_dtls " +
                     "WHERE uoi_id = '" + getUoiId() + "'"; 
        
        logger.log(Level.FINER, "!SQL: " + sql);
         
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            
            // it is ok if there are no rows to delete, not necessarily an error condition
            pStmt.executeUpdate();
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to delete restriction data", e );
                return false;
        }
        
        return true;
    }
    
    //insert new restriction
    public boolean insertRestrictions() {
        
        String sql = "INSERT INTO towner.si_restrictions_dtls (" +
                        "uoi_id, " +
                        "restrictions) " +
                      "VALUES (" +
                        "'" + getUoiId() +"', " +
                        "'" + getRestrictions() +"')";
        
        logger.log(Level.FINER, "!SQL: " + sql);
         
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Insert restriction data", e );
                return false;
        }
        
        return true;
    }
}
