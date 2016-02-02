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
import edu.si.CDIS.Database.CDISMap;
import java.sql.ResultSet;


/**
 *
 * @author rfeldman
 */



public class CDISError {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String cdisErrorCd;
    Integer cdisMapId;
    Integer cdisErrorId;
    String fileName;
     
    
    public String getCdisErrorCd() {
        return this.cdisErrorCd;
    }
    
    public Integer getCdisErrorId() {
        return this.cdisErrorId;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
        
    public void setCdisErrorCd (String cdisErrorCd) {
        this.cdisErrorCd = cdisErrorCd;
    }
    
    public void setCdisErrorId (Integer cdisErrorId) {
        this.cdisErrorId = cdisErrorId;
    }
    
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    


    public boolean insertError () {
        
        String sql = "INSERT INTO cdis_error ( " +
                        "cdis_error_id, " +
                        "cdis_map_id, " +
                        "fileName, " +
                        "cdis_error_cd, " +
                        "error_dt, " +
                        "operation_type ) " +
                    "values ( " + 
                        "cdis_error_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + getFileName() + "'," +
                        "'" + getCdisErrorCd() + "'," +
                        "SYSDATE, " +
                        "'" + CDIS.getOperationType() + "')";
       
        logger.log(Level.FINER, "SQL: " + sql ); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            
            int rowsUpdated = pStmt.executeUpdate(); 
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_ERROR table", e );
                return false;
        }
        return true;
        
    }
    
    public boolean populateCdisMapId () {
  
        String sql = "SELECT cdisMapId " +
                    "FROM cdis_error " +
                    "WHERE cdis_error_id = " + getCdisErrorId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
        
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_map_id cdis_error", e );
                return false;
        }
        return true;
    }
    
    public String returnDescription () {
        String description = null;

        String sql = "SELECT a.description " +
                     "FROM cdis_error_code_r a, " + 
                     "     cdis_error b " +
                     "WHERE a.cdis_error_cd = b.cdis_error_cd " +
                     "AND b.cdis_error_id = " + getCdisErrorId();
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
