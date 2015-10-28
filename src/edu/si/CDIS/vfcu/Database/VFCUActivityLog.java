/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VFCUActivityLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String vfcuStatusCd;
    Integer vfcuMediaFileId;
    
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    public String getVfcuStatusCd () {
        return this.vfcuStatusCd;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    public void setVfcuStatusCd (String vfcuStatusCd) {
        this.vfcuStatusCd = vfcuStatusCd;
    }
            
    public boolean insertRow (Connection damsConn) {
        
        Integer rowsInserted = 0;
        PreparedStatement pStmt = null;
        
        String sql = "INSERT INTO vfcu_activity_log ( " +
                        "vfcu_activity_log_id, " +
                        "vfcu_media_file_id, " +
                        "vfcu_status_cd, " +
                        "activity_dt ) " +
                    "VALUES (" +  
                        "vfcu_activity_log_id_seq.NextVal, " + 
                        getVfcuMediaFileId() + ", " +
                        "'" + getVfcuStatusCd() + "', " +
                        "SYSDATE )"; 
        
        try {
        
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsInserted = pStmt.executeUpdate(sql); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_activity_log", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    
}
