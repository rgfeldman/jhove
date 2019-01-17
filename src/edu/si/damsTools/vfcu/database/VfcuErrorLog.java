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
import edu.si.damsTools.utilities.XmlUtils;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class VfcuErrorLog {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String addlErrorInfo;
    private String fileName;
    private String vfcuErrorCd;
    private Integer vfcuErrorLogId;
    private Integer vfcuMd5FileId;
    private Integer vfcuMediaFileId;
   
    public String getAddlErrorInfo() {
        return this.addlErrorInfo == null ? "" : this.addlErrorInfo;
    }
    
    public String getFileName () {
        return this.fileName;
    }
   
    public String getVfcuErrorCd () {
        return this.vfcuErrorCd;
    }
    
    public Integer getVfcuErrorLogId () {
        return this.vfcuErrorLogId;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
      
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    public void setAddlErrorInfo (String addlErrorInfo) {
        this.addlErrorInfo = addlErrorInfo;
    }
        
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
     
    public void setVfcuErrorCd (String vfcuErrorCd) {
        this.vfcuErrorCd = vfcuErrorCd;
    }
    
    public void setVfcuErrorLogId (Integer vfcuErrorLogId) {
        this.vfcuErrorLogId = vfcuErrorLogId;
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
                        "addl_error_info, " +
                        "error_dt ) " +
                    "VALUES (" +  
                        "vfcu_error_id_seq.NextVal, " +
                        "'" + XmlUtils.getConfigValue("projectCd") + "', " +
                        getVfcuMediaFileId() + ", " +
                        getVfcuMd5FileId() + ", " +
                        "'" + getFileName() + "', " +
                        "'" + getVfcuErrorCd() + "', " +
                        "'" + getAddlErrorInfo() + "', " +
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
    
    
    public boolean populateDescriptiveInfo () {

        String sql = "SELECT file_name, vfcu_error_cd, addl_error_info " +
                     "FROM vfcu_error_log " + 
                     "WHERE vfcu_error_log_id =  " + getVfcuErrorLogId();
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                this.fileName = rs.getString(1);
                this.vfcuErrorCd = rs.getString(2);
                this.addlErrorInfo = rs.getString(3);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
            return false;
        }
        
        return true;

    }
    
    public ArrayList<String> returnDescriptionsForMediaId () {
        
        ArrayList<String> descriptionList = new ArrayList<>();

        String sql = "SELECT vec.description " +
                     "FROM vfcu_error_code_r vec " + 
                     "INNER JOIN vfcu_error_log vel " +
                     "ON vec.vfcu_error_cd = vel.vfcu_error_cd " +
                     "AND vel.vfcu_media_file_id = " + this.vfcuMediaFileId;
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            while (rs.next()) {
                descriptionList.add(rs.getString(1));
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to get error description", e );
        }
        return descriptionList;
    }
    
    
    
    public String returnErrDescriptionForErrorCd () {

        String errorDescription = null;
        
        String sql = "SELECT description " +
                     "FROM vfcu_error_code_r " + 
                     "WHERE vfcu_error_cd = '" + getVfcuErrorCd() + "'";
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                errorDescription = rs.getString(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
        }
        
        return errorDescription;

    }
    
}
