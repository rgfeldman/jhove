/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class VfcuErrorLog {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String fileName;
    private String vfcuErrorCd;
    private Integer vfcuErrorId;
    private Integer vfcuMd5FileId;
    private Integer vfcuMediaFileId;
   
    public String getFileName () {
        return this.fileName;
    }
   
    public String getVfcuErrorCd () {
        return this.vfcuErrorCd;
    }
    
    public Integer getVfcuErrorId () {
        return this.vfcuErrorId;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
      
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
        
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
     
    public void setVfcuErrorCd (String vfcuErrorCd) {
        this.vfcuErrorCd = vfcuErrorCd;
    }
    
    public void setVfcuErrorId (Integer vfcuErrorId) {
        this.vfcuErrorId = vfcuErrorId;
    }
            
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
      
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    public boolean insertRecord () {
        Integer rowsInserted = 0;
        
        String sql = "INSERT INTO vfcu_error_log ( " +
                        "vfcu_error_log_id, " +
                        "project_cd, " +
                        "vfcu_media_file_id, " +
                        "vfcu_md5_file_id, " +
                        "file_name, " +
                        "vfcu_error_cd, " +
                        "error_dt ) " +
                    "VALUES (" +  
                        "vfcu_error_id_seq.NextVal, " +
                        "'" + DamsTools.getProjectCd() + "', " +
                        getVfcuMediaFileId() + ", " +
                        getVfcuMd5FileId() + ", " +
                        "'" + getFileName() + "', " +
                        "'" + getVfcuErrorCd() + "', " +
                        "SYSDATE )"; 
        
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {
        
            rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to insert into vfcu_error", e );
            return false;
        } 
        return true;
    }    
    
    
    public String returnDescriptionForMediaFileId () {
        String description = null;

        String sql = "SELECT a.description " +
                        "FROM vfcu_error_code_r a, " + 
                        "       vfcu_error_log b " +
                        "WHERE a.vfcu_error_cd = b.vfcu_error_cd " +
                        "AND b.vfcu_media_file_id = " + getVfcuMediaFileId();
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                description = rs.getString(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
        }
        return description;
    }
    
    public boolean populateVfcuMediaFileId () {

        String sql = "SELECT vfcu_media_file_id " +
                        "FROM vfcu_error_log " + 
                        "WHERE vfcu_error_log_id = " + getVfcuErrorId();
        
        logger.log(Level.FINEST,"SQL! " + sql);  
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {

            if (rs.next()) {
                this.vfcuMediaFileId = rs.getInt(1);
            }
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
                return false;
        }
        return true;
  
    }
    
}
