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
public class CdisCisMediaTypeR {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer cdisCisMediaTypeId;
    String parentChildTransform;
    
    public Integer getCdisCisMediaTypeId() {
        return this.cdisCisMediaTypeId;
    }
    
    public String getParentChildTransform() {
        return this.parentChildTransform;
    }
    
    public void setCdisCisMediaTypeId(Integer cdisCisMediaTypeId) {
        this.cdisCisMediaTypeId = cdisCisMediaTypeId;
    }
    
    public void setParentChildTransform(String parentChildTransform) {
        this.parentChildTransform = parentChildTransform;
    }
    
    public boolean populateIdFromFileName (String fileName) {
        
        String sql = "SELECT cdis_cis_media_type_id " +
                    "FROM cdis_cis_media_type_r " +
                    "WHERE REGEXP_LIKE ('" + fileName + "', parent_child_transform, 'i')" +
                    " AND cdis_cis_media_type_id in (" + CDIS.getProperty("masterCisMediaTypeId") + ")";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setCdisCisMediaTypeId(rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_cis_media_type_id ", e );
                return false;
        }
        return true;
    }
    
}
