/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMd5FileActivityLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
     
    Integer vfcuMd5ActivityLogId;
    Integer vfcuMd5FileId;
    String vfcuMd5StatusCd;
    
    public void setVfcuMd5ActivityLogId (Integer vfcuMd5ActivityLogId) {
        this.vfcuMd5ActivityLogId = vfcuMd5ActivityLogId;
    }
      
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
        
    public void setVfcuMd5StatusCd (String vfcuMd5StatusCd) {
        this.vfcuMd5StatusCd = vfcuMd5StatusCd;
    }
    
    
    
    public boolean insertRecord () {
        
        String sql = "INSERT INTO vfcu_md5_file_activity_log ( " +
                        "vfcu_md5_file_activity_log_id, " +
                        "vfcu_md5_file_id, " +
                        "vfcu_md5_status_cd, " +
                        "vfcu_md5_file_status_dt) " +
                    "VALUES (" +
                        " vfcu_md5_file_act_log_id_seq.NextVal, " +
                        this.vfcuMd5FileId + ", " +
                        "'" + this.vfcuMd5StatusCd + "'," +
                        "SYSDATE ) ";
            
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_activity_log table", e );
                return false;
        }
        return true;
    }
            
}
