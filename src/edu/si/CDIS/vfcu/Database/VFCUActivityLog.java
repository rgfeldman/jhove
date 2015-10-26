/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VFCUActivityLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    
    public boolean insertRow (Connection damsConn) {
        
        Integer rowsInserted = 0;
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        String sql = "INSERT INTO vfcu_activity_log ( " +
                        "vfcu_activity_log_id, " +
                        "vfcu_file_batch_id, " +
                        "vfcu_status_cd, " +
                        "activity_dt ) " +
                    "VALUES (" +  
                        "vfcu_activity_log_id_seq.NextVal, " + 
                        "vfcu_file_batch_id" + 
                        "vfcu_status_cd " +
                        "SYSDATE )"; 
                
        
        
        try {
            
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
