/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class VFCUMd5File {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    private String vendorFilePath;
    private String vendorMd5FileName;
    private Integer vfcuMd5FileId;

    public String getVendorFilePath () {
        return vendorFilePath;
    }
        
    public String getVendorMd5FileName () {
        return vendorMd5FileName;
    }
    
    public Integer getVfcuMd5FileId () {
        return vfcuMd5FileId;
    }
    
    public void setVendorFilePath (String vendorFilePath) {
        vendorFilePath = this.vendorFilePath;
    }
    
    public void setVendorMd5FileName (String vendorMd5FileName) {
        vendorMd5FileName = this.vendorMd5FileName;
    }
    
    
    public boolean insertRecord (Connection damsConn) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            //Generate the ID for the primary key for this table
            String sql = "SELECT vfcu_md5_file_id_seq.NextVal FROM dual"; 
            
            pStmt = damsConn.prepareStatement(sql);
            pStmt.executeUpdate(sql);
            
            if (rs.next()) {
                vfcuMd5FileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_file table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        Integer rowsInserted = 0;
        try {
            
            String sql = "INSERT INTO vfcu_md5_file ( " +
                            "vfcu_md5_file_id, " +
                            "vendor_md5_file_name, " +
                            "vendor_file_path, " +
                            "md5_file_retrieval_dt ) " +
                        "VALUES (" +
                            vfcuMd5FileId + ", " +
                            "'" + getVendorMd5FileName() + "'," +
                            "'" + getVendorFilePath() + "'," +
                            "SYSDATE )"; 
            
            pStmt = damsConn.prepareStatement(sql);
            rowsInserted = pStmt.executeUpdate(sql); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_file table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
    
    public boolean findExistingMd5File (Connection damsConn) {
        
        String sql = "SELECT 'X' FROM vfcu_md5_file " + 
                     "WHERE vendor_md5_file_name = '" + getVendorMd5FileName() + "' " +
                     "AND vendor_file_path = '" + getVendorFilePath() + "'";

        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                return true;
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
    }
    

}
