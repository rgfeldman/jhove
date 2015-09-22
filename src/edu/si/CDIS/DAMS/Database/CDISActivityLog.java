/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISActivityLog {
     private final static Logger logger = Logger.getLogger(CDIS.class.getName());
     
     public boolean insertActivity (Connection damsConn, Integer cdisMapId, String cdisStatusCd) {
        
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql = "INSERT INTO cdis_activity_log ( " +
                        "cdis_activity_log_id, " +
                        "cdis_map_id, " +
                        "cdis_status_cd, " +
                        "activity_dt) " +
                    "values ( " + 
                        "cdis_activity_log_id_seq.NextVal, " +
                        cdisMapId + ", " +
                        "'" + cdisStatusCd + "', " +
                        "SYSDATE)";
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
            
         } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert CDIS_activity_log table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
     
     
}
