/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import edu.si.CDIS.DAMS.Database.CDISMap;


/**
 *
 * @author rfeldman
 */
public class CDISError {
     private final static Logger logger = Logger.getLogger(CDIS.class.getName());
     
     public boolean insertError (Connection damsConn, Integer cdisMapId, String errorCd) {
        
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql = "INSERT INTO cdis_error ( " +
                        "cdis_map_id, " +
                        "error_cd, " +
                        "error_dt) " +
                    "values ( " + 
                        cdisMapId + ", " +
                        "'" + errorCd + "'," +
                        "SYSDATE)";
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_ERROR in table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
    
}
