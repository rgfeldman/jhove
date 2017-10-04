/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisObjectMap {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer cdisMapId;
    private String cisUniqueObjectId;
    
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
    
    public String getCisUniqueObjectId () {
        return this.cisUniqueObjectId;
    }
    
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCisUniqueObjectId (String cisUniqueObjectId) {
        this.cisUniqueObjectId = cisUniqueObjectId;
    }
    
    public boolean createRecord() {
             
        String sql = "INSERT INTO cdis_object_map (" +
                        "cdis_object_map_id, " +
                        "cdis_map_id, " +
                        "cis_unique_object_id )" +
                    "VALUES (" +
                        "cdis_object_map_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + getCisUniqueObjectId() + "')";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_OBJECT_MAP table", e );
                return false;
        }      
        
        return true;
    }
    
    public boolean populateCisUniqueObjectIdforCdisId () {
  
        String sql = "SELECT cis_unique_object_id " +
                    "FROM cdis_object_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setCisUniqueObjectId (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cis_unique_object_id for cdis_map_id", e );
                return false;
        }
        return true;
    }
    
}
