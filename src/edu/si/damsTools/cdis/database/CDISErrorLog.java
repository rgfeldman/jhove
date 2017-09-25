/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.ResultSet;


/**
 *
 * @author rfeldman
 */



public class CDISErrorLog {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    String cdisErrorCd;
    Integer cdisMapId;
    Integer cdisErrorLogId;
    String fileName;
     
    
    public String getCdisErrorCd() {
        return this.cdisErrorCd;
    }
    
    public Integer getCdisErrorLogId() {
        return this.cdisErrorLogId;
    }
    
    public String getFileName() {
        return this.fileName == null ? "" : this.fileName;
    }
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
        
    public void setCdisErrorCd (String cdisErrorCd) {
        this.cdisErrorCd = cdisErrorCd;
    }
    
    public void setCdisErrorId (Integer cdisErrorId) {
        this.cdisErrorLogId = cdisErrorId;
    }
    
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    


    public boolean insertError () {
        
        String sql = "INSERT INTO cdis_error_log ( " +
                        "cdis_error_log_id, " +
                        "cdis_map_id, " +
                        "project_cd, " +
                        "file_name, " +
                        "cdis_error_cd, " +
                        "error_dt, " +
                        "operation_type ) " +
                    "values ( " + 
                        "cdis_error_log_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + DamsTools.getProjectCd() + "', " +
                        "'" + getFileName() + "', " +
                        "'" + getCdisErrorCd() + "', " +
                        "SYSDATE, " +
                        "'" + DamsTools.getOperationType() + "')";
       
        logger.log(Level.FINER, "SQL: " + sql ); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            
            int rowsUpdated = pStmt.executeUpdate(); 
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_ERROR_LOG table", e );
                return false;
        }
        return true;
        
    }
    
    public boolean populateCdisMapId () {
  
        String sql = "SELECT cdis_map_Id " +
                    "FROM cdis_error_log " +
                    "WHERE cdis_error_log_id = " + getCdisErrorLogId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
        
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_map_id from cdis_error_log", e );
                return false;
        }
        return true;
    }
    
    
    
    public String returnDescription () {
        String description = null;

        String sql = "SELECT a.description " +
                     "FROM cdis_error_code_r a, " + 
                     "     cdis_error_log b " +
                     "WHERE a.cdis_error_cd = b.cdis_error_cd " +
                     "AND b.cdis_error_log_id = " + getCdisErrorLogId();
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                description = rs.getString(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to get error description", e );
        }
        return description;
    }
    
    public String returnDescriptionForMapId () {
        String description = null;

        String sql = "SELECT a.description " +
                     "FROM cdis_error_code_r a, " + 
                     "     cdis_error_log b " +
                     "WHERE a.cdis_error_cd = b.cdis_error_cd " +
                     "AND b.cdis_map_id = " + getCdisMapId();
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                description = rs.getString(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to get error description", e );
        }
        return description;
    }
    
    
}
