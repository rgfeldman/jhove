/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMd5FileHierarchy {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer masterFileVfcuMd5FileId;
    private Integer subFileVfcuMd5FileId;
    private Integer subSubFileVfcuMd5FileId;
    
    public Integer getMasterFileVfcuMd5FileId() {
        return this.masterFileVfcuMd5FileId;
    }
    
    public Integer getSubFileVfcuMd5FileId() {
        return this.subFileVfcuMd5FileId;
    }
       
    public Integer getSubSubFileVfcuMd5FileId() {
        return this.subSubFileVfcuMd5FileId;
    }
    
    public void setMasterFileVfcuMd5FileId(Integer masterFileVfcuMd5FileId) {
        this.masterFileVfcuMd5FileId = masterFileVfcuMd5FileId;
    }
    
    public void setSubFileVfcuMd5FileId(Integer subFileVfcuMd5FileId) {
        this.subFileVfcuMd5FileId = subFileVfcuMd5FileId;
    }
    
    public void setSubSubFileVfcuMd5FileId(Integer subFileVfcuMd5FileId) {
        this.subSubFileVfcuMd5FileId = subSubFileVfcuMd5FileId;
    }
              
    public boolean insertRow () {
        
        String sql = "INSERT INTO vfcu_md5_file_hierarchy ( " +
                        "vfcu_md5_file_hierarchy_id, " +
                        "masterfile_vfcu_md5_file_id, " +
                        "subfile_vfcu_md5_file_id, " +
                        "sub_subfile_vfcu_md5_file_id) " +
                    "VALUES (" +
                        "vfcu_md5_file_hierarchy_id_seq.NextVal, " + 
                        getMasterFileVfcuMd5FileId() + ", " +
                        getSubFileVfcuMd5FileId()  + ", " +
                        getSubSubFileVfcuMd5FileId() + ")";
    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) )  {
            logger.log(Level.FINEST, "SQL: {0}", sql);

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_md5_file_hierarchy", e );
                return false;
        }
        return true;
    }
    
     public boolean updateSubFileVfcuMd5FileId () {
        
        String sql = "UPDATE vfcu_md5_file_hierarchy " +
                        "SET subfile_vfcu_md5_file_id = " + getSubFileVfcuMd5FileId() +
                        " WHERE masterfile_vfcu_md5_file_id = " + getMasterFileVfcuMd5FileId();
    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) )  {
            logger.log(Level.FINEST, "SQL: {0}", sql);

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update vfcu_md5_file_hierarchy", e );
                return false;
        }
        return true;
    }
     
    public boolean updateSubSubFileVfcuMd5FileId () {
        
        String sql = "UPDATE vfcu_md5_file_hierarchy " +
                        "SET sub_subfile_vfcu_md5_file_id = " + getSubSubFileVfcuMd5FileId() +
                        " WHERE masterfile_vfcu_md5_file_id = " + getMasterFileVfcuMd5FileId();
    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) )  {
            logger.log(Level.FINEST, "SQL: {0}", sql);

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update vfcu_md5_file_hierarchy", e );
                return false;
        }
        return true;
    }
    
}
