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

public class CDISActivityLog {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer cdisActivityLogId;
    private Integer cdisMapId;
    private String cdisStatusCd;

    
    public Integer getCdisMapId() {
        return this.cdisMapId;
    }
    
    public Integer getCdisActivityLogId() {
        return this.cdisActivityLogId;
    }
       
    public String getCdisStatusCd() {
        return this.cdisStatusCd;
    }
    
    public void setCdisStatusCd(String cdisStatusCd) {
        this.cdisStatusCd = cdisStatusCd;
    }
    
    public void setCdisMapId(Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCdisActivityLogId(Integer cdisActivityLogId) {
        this.cdisActivityLogId = cdisActivityLogId;
    }
        
     
    public boolean insertActivity () {
        
        int rowsUpdated = 0;
        
        String sql = "INSERT INTO cdis_activity_log ( " +
                        "cdis_activity_log_id, " +
                        "cdis_map_id, " +
                        "cdis_status_cd, " +
                        "operation_type, " +
                        "activity_dt) " +
                    "values ( " + 
                        "cdis_activity_log_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + getCdisStatusCd() + "', " +
                        "'" + CDIS.getOperationType() + "', " +
                        "SYSDATE)";
        logger.log(Level.FINER, "SQL: " + sql );
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            rowsUpdated = pStmt.executeUpdate(sql); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert CDIS_activity_log table", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIdFromMapIdStat () {
        
        String sql = "SELECT cdis_activity_log_id " +
                    "FROM cdis_activity_log " +
                    "WHERE cdis_map_id = " + getCdisMapId() +
                    " AND cdis_status_cd = '" + getCdisStatusCd() + "' ";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setCdisActivityLogId (rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for cis ID", e );
                return false;
        }
        return true;
    }
    
    public boolean updateActivityDtCurrent () {
        
        int rowsUpdated = 0;
        
        String sql = "UPDATE cdis_activity_log " +
                    "SET activity_dt = SYSDATE " +
                    "WHERE cdis_activity_log_id = " + getCdisActivityLogId();
        
        logger.log(Level.FINER, "SQL: " + sql );
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            rowsUpdated = pStmt.executeUpdate(sql); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update the date in the activtyLog", e );
                return false;
        }
        return true;
    }   
     
}
