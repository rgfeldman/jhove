/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISRefIdMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer cdisMapId;
    private Integer cdisRefIdMapId;
    private String refId;
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
    
    public Integer getCdisRefIdMapId () {
        return this.cdisRefIdMapId;
    }
    
    public String getRefId() {
        return this.refId;
    }
    
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCdisRefIdMapId (Integer cdisRefIdMapId) {
        this.cdisRefIdMapId = cdisRefIdMapId;
    }
        
    public void setRefId (String refId) {
        this.refId = refId;
    }
    
    
    public boolean createRecord() {
             
        String sql = "INSERT INTO cdis_refId_map (" +
                        "cdis_refid_map_id, " +
                        "cdis_map_id, " +
                        "ref_id )" +
                    "VALUES (" +
                        "cdis_refid_map_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + getRefId() + "')";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_REF_ID_MAP table", e );
                return false;
        }      
        
        return true;
    }
    
    
}
