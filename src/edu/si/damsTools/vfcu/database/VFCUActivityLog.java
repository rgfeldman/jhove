/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class VFCUActivityLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String vfcuStatusCd;
    private Integer vfcuMediaFileId;
    
    
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
            
    public boolean insertRow () {
        
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
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql))  {
            
            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_activity_log", e );
                return false;
        }
        
        return true;
    }
}
