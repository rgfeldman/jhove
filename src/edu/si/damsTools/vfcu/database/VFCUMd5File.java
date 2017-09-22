/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class VFCUMd5File {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    private String basePathStaging;
    private String basePathVendor;
    private String filePathEnding;
    private Integer masterMd5FileId;
    private String vendorMd5FileName;
    private Integer vfcuMd5FileId;
    

    public String getBasePathStaging () {
        return this.basePathStaging;
    }
        
    public String getBasePathVendor () {
        return this.basePathVendor;
    }
    
    public String getFilePathEnding () {
        return this.filePathEnding == null ? "" : this.filePathEnding;
    }
    
    public Integer getMasterMd5FileId () {
        return this.masterMd5FileId;
    }
         
    public String getVendorMd5FileName () {
        return this.vendorMd5FileName;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
   
    
    public void setBasePathStaging (String basePathStaging) {
        this.basePathStaging = basePathStaging;
    }
        
    public void setBasePathVendor (String basePathVendor) {
        this.basePathVendor = basePathVendor;
    }
    
    public void setFilePathEnding (String filePathEnding) {
        this.filePathEnding = filePathEnding;
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
    
    public boolean generateVfcuMd5FileId () {

        String sql = "SELECT vfcu_md5_file_id_seq.NextVal FROM dual"; 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery();  ){
            
            if (rs.next()) {
                this.vfcuMd5FileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to get identifier sequence for vfcu_md5_file", e );
                return false;
        }
        return true;
    }
    
    
    public boolean insertRecord () {
           
        String sql = "INSERT INTO vfcu_md5_file ( " +
                        "vfcu_md5_file_id, " +
                        "project_cd, " +
                        "vendor_md5_file_name, " +
                        "base_path_vendor, " +
                        "base_path_staging, " +
                        "file_path_ending, " +
                        "md5_file_retrieval_dt, " +
                        "master_md5_file_id) " +
                    "VALUES (" +
                        getVfcuMd5FileId() + ", " +
                        "'" + DamsTools.getProjectCd() + "', " +
                        "'" + getVendorMd5FileName() + "'," +
                        "'" + getBasePathVendor() + "'," +
                        "'" + getBasePathStaging() + "'," +
                        "'" + getFilePathEnding()  + "'," +
                        "SYSDATE, " + 
                        getMasterMd5FileId() + ") ";
            
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_file table", e );
                return false;
        }
        
        return true;
        
    }
    
    
    public boolean populateMasterMd5FileId () {

        String sql = "SELECT master_md5_file_id " +
                      "FROM vfcu_md5_file " + 
                      "WHERE vfcu_md5_file_id = " + getVfcuMd5FileId();
          
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ) {
                       
            logger.log(Level.FINEST,"SQL! " + sql); 
           
            if (rs.next()) {
                setMasterMd5FileId(rs.getInt(1));
            }
            else {
                return false;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for Staging File Path in DB", e );
            return false;
        }
        return true;
    }
    
    
    public boolean populateStagingFilePath () {

        String sql = "SELECT base_path_staging, file_path_ending " +
                      "FROM vfcu_md5_file " + 
                      "WHERE vfcu_md5_file_id = " + getVfcuMd5FileId();
          
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ) {
                       
            logger.log(Level.FINEST,"SQL! " + sql); 
           
            if (rs.next()) {
                basePathStaging = (rs.getString(1));
                filePathEnding = (rs.getString(2));
            }
            else {
                return false;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for Staging File Path in DB", e );
            return false;
        }
        return true;
    }
    
    public boolean populateVendorFilePath () {

        String sql = "SELECT base_path_vendor, file_path_ending " +
                      "FROM vfcu_md5_file " + 
                      "WHERE vfcu_md5_file_id = " + getVfcuMd5FileId();
          
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ) {
                       
            logger.log(Level.FINEST,"SQL! " + sql); 
           
            if (rs.next()) {
                basePathVendor = (rs.getString(1));
                filePathEnding = (rs.getString(2));
            }
            else {
                return false;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for Vendor File Path in DB", e );
            return false;
        }
        return true;
    }
    
    public Integer returnIdForNamePath () {

        Integer md5Id = null;
        String filePathEndingClause = null;
        

        if (getFilePathEnding() != null && ! getFilePathEnding().isEmpty()) {
            filePathEndingClause = "= '" + getFilePathEnding() + "'";
        }
        else {
            filePathEndingClause = "IS NULL";
        }
        
        String sql = "SELECT vfcu_md5_file_id FROM vfcu_md5_file " + 
                     "WHERE vendor_md5_file_name = '" + getVendorMd5FileName() + "' " +
                     "AND base_path_vendor = '" + getBasePathVendor() + "' " +
                     "AND file_path_ending " + filePathEndingClause;
                   
        logger.log(Level.FINEST,"SQL! " + sql);  
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery()) {
            
            if (rs != null && rs.next()) {
                md5Id = rs.getInt(1);
            }
            return md5Id;
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
                return -1;
        }  
    }
    
    public Integer returnMasterIdForPath () {

        Integer md5Id = null;
        String sql = "SELECT max(vfcu_md5_file_id) " +
                     "FROM vfcu_md5_file " + 
                     "WHERE master_md5_file_id = vfcu_md5_file_id " +
                     "AND base_path_vendor = '" + getBasePathVendor() + "' " +
                     "AND file_path_ending = '" + getFilePathEnding() + "' " +
                     "AND project_cd = '" + DamsTools.getProjectCd() + "'";
                   
        logger.log(Level.FINEST,"SQL! " + sql);  
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery()) {
            
            if (rs != null && rs.next()) {
                md5Id = rs.getInt(1);
            }
            return md5Id;
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Find Id for Path", e );
                return -1;
        }  
    }
    
    public ArrayList<Integer> returnMd5sWithNoMasterID () {
        
        ArrayList<Integer> masterlessMd5Ids;
        masterlessMd5Ids = new ArrayList<> ();
        
        String sql = "SELECT    vfcu_md5_file_id " + 
                      "FROM      vfcu_md5_file a " +
                      "WHERE     a.master_md5_file_id is NULL " +
                      "AND       a.project_cd = '" + DamsTools.getProjectCd() + "'";
                                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs.next()) {
                masterlessMd5Ids.add(rs.getInt(1));
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/batch", e );
        }
    
        return masterlessMd5Ids; 
    }

    
    public Integer returnSubFileMd5Id (Integer masterMd5FileId) {
        
        Integer md5SubFileId = 0;     
        String sql =    "SELECT vfcu_md5_file_id " +
                            "FROM vfcu_md5_file " +
                            "WHERE master_md5_file_id != vfcu_md5_file_id " +
                            "AND master_md5_file_id = " + getMasterMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
    
    
    public int updateMasterMd5File_id () {
        
        int rowsUpdated = 0;
        
        //order by filename so each process has an even distribution of raws and .tifs.
        //Need multiple subqueries for order by with rownum clause
        String sql = 
            "UPDATE vfcu_md5_file " +
            "SET    master_md5_file_id = " + getMasterMd5FileId() +
            " WHERE  vfcu_md5_file_id = " + getVfcuMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated " + rowsUpdated );
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update vfcu_md5_File with masterId", e );
            return -1;
        }
        return rowsUpdated;
    }
    
    public int updateVfcuRptDt () {
        
        int rowsUpdated = 0;
        
        //order by filename so each process has an even distribution of raws and .tifs.
        //Need multiple subqueries for order by with rownum clause
        String sql = 
            "UPDATE vfcu_md5_file " +
            "SET    vfcu_rpt_dt = SYSDATE " +
            "WHERE  vfcu_md5_file_id = " + getVfcuMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated" + rowsUpdated );
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update vfcu_md5_File with report date", e );
            return -1;
        }
        return rowsUpdated;
    }
    
}

