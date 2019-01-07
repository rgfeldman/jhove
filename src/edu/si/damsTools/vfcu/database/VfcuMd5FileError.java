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
public class VfcuMd5FileError {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer vfcuMd5FileId;
    private String vfcuMd5ErrorCd;
    
    public String getVfcuMd5ErrorCd() {
        return this.vfcuMd5ErrorCd;
    }
       
    public Integer getVfcuMd5FileId() {
        return this.vfcuMd5FileId;
    }

    public void setVfcuMd5ErrorCd (String vfcuMd5ErrorCd)  {
        this.vfcuMd5ErrorCd = vfcuMd5ErrorCd;
    }
        
    public void setVfcuMd5FileId (Integer vfcuMd5FileId)  {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean insertRecord () {
        Integer rowsInserted = 0;
        
        String sql = "INSERT INTO vfcu_md5_file_error_log ( " +
                        "vfcu_md5_file_error_log_id, " +
                        "vfcu_md5_file_id, " +
                        "vfcu_md5_error_cd, " +
                        "error_dt ) " +
                    "VALUES (" +  
                        "vfcu_md5_file_error_id_seq.NextVal, " +
                        getVfcuMd5FileId() + ", " +
                        "'" + getVfcuMd5ErrorCd() + "', " +
                        "SYSDATE )"; 
        
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {
        
            rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_error", e );
            return false;
        } 
        return true;
    }    
    
}
