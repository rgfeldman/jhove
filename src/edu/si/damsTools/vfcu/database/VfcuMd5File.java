/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class VfcuMd5File {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    private String basePathStaging;
    private String basePathVendor;
    private String filePathEnding;
    private String vendorMd5FileName;
    private Integer vfcuMd5FileId;
    
    public VfcuMd5File() {

    }

    public String getBasePathStaging () {
        return this.basePathStaging;
    }
        
    public String getBasePathVendor () {
        return this.basePathVendor;
    }
    
    public String getFilePathEnding () {
        return this.filePathEnding == null ? "" : this.filePathEnding;
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
   
    public void setVendorMd5FileName (String vendorMd5FileName) {
        this.vendorMd5FileName = vendorMd5FileName;
    }
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean insertRecord () {
           
        generateVfcuMd5FileId();
        
        String sql = "INSERT INTO vfcu_md5_file ( " +
                        "vfcu_md5_file_id, " +
                        "project_cd, " +
                        "vendor_md5_file_name, " +
                        "base_path_vendor, " +
                        "base_path_staging, " +
                        "file_path_ending, " +
                        "md5_file_retrieval_dt) " +
                    "VALUES (" +
                        getVfcuMd5FileId() + ", " +
                        "'" + DamsTools.getProjectCd() + "', " +
                        "'" + getVendorMd5FileName() + "'," +
                        "'" + getBasePathVendor() + "', " +
                        "'" + getBasePathStaging() + "', " +
                        "'" + getFilePathEnding()  + "', " +
                        "SYSDATE)";
            
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
    
    public void populateBasicDbData () {
        
        String sql =    "SELECT base_path_vendor, base_path_staging, file_path_ending, file_hierarchy_cd " +
                        "FROM   vfcu_md5_file " +
                        "WHERE  vfcu_md5_file_id = " + this.vfcuMd5FileId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                this.basePathVendor = rs.getString(1);
                this.basePathStaging = rs.getString(2);
                this.filePathEnding = rs.getString(3);     
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to Populate basic data for md5 from database", e );
        }
    }
    
     public void populateBasePathVendor () {
        
        String sql =    "SELECT base_path_vendor " +
                        "FROM   vfcu_md5_file " +
                        "WHERE  vfcu_md5_file_id = " + this.vfcuMd5FileId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                this.basePathVendor = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain basePathVendor", e );
        }
        
    }
    
    public void populateFilePathEnding () {
        
        String sql =    "SELECT file_path_ending " +
                        "FROM   vfcu_md5_file " +
                        "WHERE  vfcu_md5_file_id = " + this.vfcuMd5FileId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                this.filePathEnding = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain path ending", e );
        }
        
    }
      
    public Integer returnAssocMd5Id() {
        
        Integer returnVal = null;
        
        String filePathEndingToCheck = getFilePathEnding();
        
        if (filePathEndingToCheck.contains("/")) {
            filePathEndingToCheck = filePathEndingToCheck.substring(0, filePathEndingToCheck.lastIndexOf("/")+1);
        }
        else {
            filePathEndingToCheck = "";
        }
        
        //if (getFileHierarchyCd().equals("M")) {
       //     filePathEndingToCheck = filePathEndingToCheck + DamsTools.getProperty("vendorSubFileDir");
       // }
       // else {
        //     filePathEndingToCheck = filePathEndingToCheck + DamsTools.getProperty("vendorMasterFileDir");
       // }
               
        /*String sql =    "SELECT vfcu_md5_file_id " +
                        "FROM   vfcu_md5_file " +
                        "WHERE  base_path_vendor = '" + getBasePathVendor() + "' " +
                        "AND    file_path_ending = '" + filePathEndingToCheck + "' " +
                        "AND    file_hierarchy_cd != '" + getFileHierarchyCd() + "' " +
                        "AND    vfcu_md5_file_id != " + this.vfcuMd5FileId;
        */
        /*logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 returnVal = rs.getInt(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain path ending", e );
        }*/
        
        return returnVal;
        
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
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated", rowsUpdated );
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update vfcu_md5_File with report date", e );
            return -1;
        }
        return rowsUpdated;
        
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
    
     private boolean generateVfcuMd5FileId () {

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
}

