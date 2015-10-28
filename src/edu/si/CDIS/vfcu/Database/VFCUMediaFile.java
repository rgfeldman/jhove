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
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


public class VFCUMediaFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    ArrayList<Integer> filesIdsForBatch;
    Integer maxFiles;
    String  mediaFileName;
    String  vendorChecksum;
    Long    vfcuBatchNumber;
    String  vfcuChecksum;
    Integer vfcuMediaFileId;
    Integer vfcuMd5FileId;
    
   
    
    public ArrayList<Integer> getFilesIdsForBatch () {
        return this.filesIdsForBatch;
    }
    
    public String getMediaFileName () {
        return this.mediaFileName;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
    
    public String getVendorChecksum () {
        return this.vendorChecksum;
    }
    
    public Long getVfcuBatchNumber () {
        return this.vfcuBatchNumber;
    }
    
    public String getVfcuChecksum () {
        return this.vfcuChecksum;
    }
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    
 
    public void setMaxFiles (Integer maxFiles) {
        this.maxFiles = maxFiles;
    }
    
    public void setMediaFileName (String mediaFileName) {
        this.mediaFileName = mediaFileName;
    }
        
    public void setVendorCheckSum (String vendorCheckSum) {
        this.vendorChecksum = vendorCheckSum;
    }
    
    public void setVfcuBatchNumber (Long vfcuBatchNumber) {
        this.vfcuBatchNumber = vfcuBatchNumber;
    }
    
    public void setVfcuChecksum (String vfcuChecksum) {
        this.vfcuChecksum = vfcuChecksum;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean insertRow (Connection damsConn) {
        
        Integer rowsInserted = 0;
        PreparedStatement pStmt = null;
        
        String sql = "INSERT INTO vfcu_media_file ( " +
                        "vfcu_media_file_id, " +
                        "vfcu_md5_file_id, " +
                        "media_file_name, " +
                        "vendor_checksum) " +
                    "VALUES (" +
                        "vfcu_media_file_id_seq.NextVal, " +
                        getVfcuMd5FileId() + "'," +
                        "'" + getMediaFileName() + "'," +
                        "'" + getVendorChecksum() + "')";
    
        try {
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsInserted = pStmt.executeUpdate(sql); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_media_file", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public boolean getFileIdsCurrentBatch (Connection damsConn) {
    
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        filesIdsForBatch = new ArrayList<>();
        
        try {
            String sql = "SELECT  vfcu_media_file_id " +
                        "FROM     vfcu_media_file " +
                        "WHERE    vfcu_batch_number = " + getVfcuBatchNumber() + " ";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            pStmt.executeUpdate(sql);
            
            while (rs.next()) {
                 filesIdsForBatch.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_file table", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
         
        return true;
    }
    
    public boolean updateVfcuBatchNumber (Connection damsConn) {
        
        Integer rowsUpdated = 0;
        PreparedStatement pStmt = null;
        
        try {
            
            String sql = "UPDATE    vfcu_media_file" +
                         "SET       vfcu_batch_number = " + getVfcuBatchNumber() + " " +
                         "WHERE     vfcu_batch_number IS NULL " + 
                         "AND       rownum < " + maxFiles;
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated= pStmt.executeUpdate(sql); 
            
            logger.log(Level.FINER, "Rows updated for current batch", rowsUpdated );
            
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update batch number in vfcu_file_batch", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    
    public boolean updateVfcuChecksum (Connection damsConn) {
       
        Integer rowsUpdated = 0;
        PreparedStatement pStmt = null;
        
        try {
          
            String sql = "UPDATE    vfcu_media_file" +
                         "SET       vfcu_checksum = '" + getVfcuChecksum() + "' " +
                         "WHERE     vfcu_media_file_id = " + getVfcuMediaFileId();
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated= pStmt.executeUpdate(sql); 
            
            logger.log(Level.FINER, "Rows updated for current batch", rowsUpdated );
            
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update checksum in vfcu_file_batch", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
}
