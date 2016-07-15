/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VFCUMd5File {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    private Integer masterMd5FileId;
    private String vendorMd5FileName;
    private Integer vfcuMd5FileId;
    
    
    public Integer getMasterMd5FileId () {
        return this.masterMd5FileId;
    }
         
    public String getVendorMd5FileName () {
        return this.vendorMd5FileName;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
   
    
    public void setMasterMd5FileId (Integer masterMd5FileId) {
        this.masterMd5FileId = masterMd5FileId;
    }
   
    public void setVendorMd5FileName (String vendorMd5FileName) {
        this.vendorMd5FileName = vendorMd5FileName;
    }
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public int updateCdisRptDt () {
        
        int rowsUpdated = 0;
        
        //order by filename so each process has an even distribution of raws and .tifs.
        //Need multiple subqueries for order by with rownum clause
        String sql = 
            "UPDATE vfcu_md5_file " +
            "SET    cdis_rpt_dt = SYSDATE " +
            "WHERE  vfcu_md5_file_id = " + getVfcuMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated", rowsUpdated );
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update vfcu_md5_File with report date", e );
            return -1;
        }
        return rowsUpdated;
        
    }
    
    public Integer returnSubFileMd5Id (Integer masterMd5FileId) {
        
        Integer md5SubFileId = 0;     
        String sql =    "SELECT vfcu_md5_file_id " +
                            "FROM vfcu_md5_file " +
                            "WHERE master_md5_file_id != vfcu_md5_file_id " +
                            "AND master_md5_file_id = " + getMasterMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
           
            while (rs.next()) {
                 md5SubFileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain child md5 ID from vfcu_md5_file", e );
            return -1;
        }
        
        return md5SubFileId;
    }
}
