/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class SiPreservationMetadata {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
 
    private String assetSourceDate;
    private String preservationIdNumber;
    private String uoiid;
    
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
        
        String sql = "INSERT INTO towner.si_preservation_metadata (" +
                        "uoi_id, " +
                        "preservation_id_number, " +
                        "preservation_id_type) " +
                      "VALUES (" +
                        "'" + getUoiid() +"', " +
                        "'" + getPreservationIdNumber() +"', " +
                        "'md5')";
        
        logger.log(Level.FINER, "!SQL: " + sql);
         
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Insert Preservation data", e );
                return false;
        }
        
        return true;
                        
    }
}
