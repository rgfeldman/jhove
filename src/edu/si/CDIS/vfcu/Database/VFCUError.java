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
public class VFCUError {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer vfcuMediaFileId;
    String vfcuErrorCd;
    
    public String getVfcuErrorCd () {
        return this.vfcuErrorCd;
    }
      
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    public void setVfcuErrorCd (String vfcuErrorCd) {
        this.vfcuErrorCd = vfcuErrorCd;
    }
        
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    public boolean insertRecord (Connection damsConn) {
        Integer rowsInserted = 0;
        PreparedStatement pStmt = null;
        
        String sql = "INSERT INTO vfcu_error ( " +
                        "vfcu_error_id, " +
                        "vfcu_media_file_id, " +
                        "vfcu_error_cd, " +
                        "error_dt ) " +
                    "VALUES (" +  
                        "vfcu_error_log_id_seq.NextVal, " + 
                        getVfcuMediaFileId() + ", " +
                        "'" + getVfcuErrorCd() + "', " +
                        "SYSDATE )"; 
        
        try {
        
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsInserted = pStmt.executeUpdate(sql); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_error", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }    
    
}
