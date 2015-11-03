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
import edu.si.CDIS.vfcu.MediaFile;


public class VFCUMediaFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    ArrayList<Integer> filesIdsForBatch;
    Integer maxFiles;
    String  mediaFileName;
    String  vendorChecksum;
    Long    vfcuBatchNumber;
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
                        getVfcuMd5FileId() + "," +
                        "'" + getMediaFileName() + "'," +
                        "UPPER ('" + getVendorChecksum() + "'))";
    
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
            rs = pStmt.executeQuery();
            
            while (rs.next()) {
                 filesIdsForBatch.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
         
        return true;
    }
    
    public int updateVfcuBatchNumber (Connection damsConn) {
        
        int rowsUpdated = 0;
        PreparedStatement pStmt = null;
        
        try {
            
            //order by filename so each process has an even distribution of raws and .tifs.
            //Need multiple subqueries for order by with rownum clause
            String sql = 
                    "UPDATE vfcu_media_file a " +
                    "SET    a.vfcu_batch_number = " + getVfcuBatchNumber() + " " +
                    "WHERE  a.vfcu_media_file_id IN ( " +
                        "SELECT vfcu_media_file_id " + 
                        "FROM ( " +
                            "Select * " +
                            "FROM vfcu_media_file b " +
                            "WHERE  b.vfcu_batch_number IS NULL " +
                            "ORDER BY b.media_file_name ) " +
                            " where rownum < " + this.maxFiles + "+ 1) ";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated= pStmt.executeUpdate(sql); 
            
            logger.log(Level.FINER, "Rows updated for current batch", rowsUpdated );
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update batch number in vfcu_file_batch", e );
                return -1;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return rowsUpdated;
    }
    
    
    public boolean updateVfcuChecksumAndDate (Connection damsConn, MediaFile mediaFile) {
       
        Integer rowsUpdated = 0;
        PreparedStatement pStmt = null;
        
        try {
          
            String sql = "UPDATE    vfcu_media_file " +
                         "SET       vfcu_checksum = UPPER('" + mediaFile.getVfcuMd5Hash() + "'), " +
                         "          media_file_date = TO_DATE ('" + mediaFile.getFileDate() + "','YYYY-MM-DD') " +
                         "WHERE     vfcu_media_file_id = " + mediaFile.getVfcuMediaFileId();
            
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
    
    public boolean populateVendorChecksum (Connection damsConn) {
    
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT  vendor_checksum " +
                        "FROM     vfcu_media_file " +
                        "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                 setVendorCheckSum(rs.getString(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
         
        return true;
    }
    
    public int countNumFilesForMd5ID (Connection damsConn) {
        int numFiles = 0; 
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT  count(*) " +
                        "FROM     vfcu_media_file " +
                        "WHERE    vfcu_media_file_id = " + getVfcuMd5FileId() + " ";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                 numFiles = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return -1;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
         
        
        return numFiles;
    }
    
}
