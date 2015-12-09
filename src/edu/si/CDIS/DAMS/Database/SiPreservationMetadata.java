/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

/**
 *
 * @author rfeldman
 */
public class SiPreservationMetadata {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
 
    String assetSourceDate;
    String preservationIdNumber;
    String uoiid;
    
    public String getAssetSourceDate() {
        return this.assetSourceDate;
    }
    
    public String getUoiid () {
        return this.uoiid;
    } 
    
    public String getPreservationIdNumber () {
        return this.preservationIdNumber;
    }
    
    public void setAssetSourceDate(String assetSourceDate) {
        this.assetSourceDate = assetSourceDate;
    }
    
    public void setPreservationIdNumber(String preservationIdNumber) {
        this.preservationIdNumber = preservationIdNumber;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
    
    public boolean insertRow () {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        int rowsUpdated = 0;
        
        String sql = "INSERT INTO towner.si_preservation_metadata (" +
                        "uoi_id, " +
                        "preservation_id_number, " +
                        "preservation_id_type) " +
                      "VALUES (" +
                        "'" + getUoiid() +"', " +
                        "'" + getPreservationIdNumber() +"', " +
                        "'md5')";
        
        logger.log(Level.FINER, "!SQL: " + sql);
         
        try {
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Insert Preservation data", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        } 
        
        return true;
                        
    }
    
}
