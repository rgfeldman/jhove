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


public class VFCUFileBatch {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String  mediaFileName;
    String  vendorChecksum;
    Long    vfcuBatchNumber;
    Integer vfcuMd5FileId;
    
    
    
    public String getMediaFileName () {
        return mediaFileName;
    }
    
    public Integer getVfcuMd5FileId () {
        return vfcuMd5FileId;
    }
    
    public String getVendorChecksum () {
        return vendorChecksum;
    }
    
    public Long getVfcuBatchNumber () {
        return vfcuBatchNumber;
    }
    
    public void setMediaFileName (String mediaFileName) {
        this.mediaFileName = mediaFileName;
    }
        
    public void setVendorCheckSum (String vendorCheckSum) {
        this.vendorChecksum = vendorCheckSum;
    }
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean insertRow (Connection damsConn) {
        
        Integer rowsInserted = 0;
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        String sql = "INSERT INTO vfcu_file_batch ( " +
                        "vfcu_file_batch_id, " +
                        "vfcu_md5_file_id, " +
                        "media_file_name, " +
                        "vendor_checksum) " +
                    "VALUES (" +
                        "vfcu_file_batch_seq.NextVal, " +
                        getVfcuMd5FileId() + "'," +
                        "'" + getMediaFileName() + "'," +
                        "'" + getVendorChecksum() + "')";
    
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsInserted = pStmt.executeUpdate(sql); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_file_batch", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
}
