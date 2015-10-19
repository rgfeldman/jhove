/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISForIngest {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String hotFolder;
    String cisUniqueMediaId;
    String siHoldingUnit;
    
    public String getHotFolder () {
        return this.hotFolder;
    }
    
    public String getCisUniqueMediaId () {
        return this.cisUniqueMediaId;
    }
    
    public String getSiHoldingUnit () {
        return this.siHoldingUnit;
    }
    
    public void setHotFolder (String hotFolder) {
        this.hotFolder = hotFolder;
    }
    
    public void setCisUniqueMediaId (String cisUniqueMediaId) {
        this.cisUniqueMediaId = cisUniqueMediaId;
    }
    
    public void setSiHoldingUnit (String siHoldingUnit) {
        this.siHoldingUnit = siHoldingUnit;
    }
    
    
    
    public boolean populateHotFolder (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT hot_folder " +
                    "FROM cdis_for_ingest " +
                    "WHERE cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                    "AND si_holding_unit = '" + getSiHoldingUnit() + "'";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setHotFolder (rs.getString(1));
            }   
            else {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain HotFolder name from cdis_for_ingest", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return true;
    }
      
}
