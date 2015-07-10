/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

/**
 *
 * @author rfeldman
 */
import edu.si.CDIS.CDIS;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;

public class CDISMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer CDISid;
    
    public Integer getCdisMapId () {
        return this.CDISid;
    }
    
    public void setCdisMapId (Integer CDISid) {
        this.CDISid = CDISid;
    }
    
    public boolean getMapIDFileBatch () {
        
        return true;
    }
    
    public boolean updateStatus (Connection damsConn, char newStatus) {
        
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql = "UPDATE cdis_map " +
                    "SET cdis_status_cd = '" + newStatus + "' " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
            
         } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP status in table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
    
    public boolean createRecord(CDIS cdis, String cisId) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        int rowsUpdated = 0;
  
        String sql = "SELECT cdis_id_seq.NextVal from dual";
        
         try {
            
            pStmt = cdis.damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();

            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }    
            
        
            sql =  "INSERT INTO cdis_map (" +
                            "cdis_map_id, " +
                            "si_unit, " +
                            "cis_id, " +
                            "batch_number, " +
                            "map_entry_dt, " +
                            "cdis_status_cd) " +
                        "VALUES (" +
                            getCdisMapId() + ", " +
                            "'" + cdis.properties.getProperty("siUnit") + "', " +
                            "'" + cisId + "', " +
                            cdis.getBatchNumber() + ", " +
                            "SYSDATE, " +
                            "'R')";
                 
            logger.log(Level.FINEST,"SQL! " + sql);  
        
            pStmt = cdis.damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
                
        return true;
       
    }
    
}
