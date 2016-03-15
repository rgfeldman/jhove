/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Level;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;
import java.util.HashMap;



public class SiAssetMetaData {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    //class attributes
    private String owningUnitUniqueName;
    private String sourceSystemId;
    private String uoiid;  
    private String isRestricted;
    
    public HashMap <String,Integer> metaDataDBLengths; 
      
    public String getIsRestricted() {
        return this.isRestricted;
    }
     
    public String getOwningUnitUniqueName() {
        return this.owningUnitUniqueName;
    }
    
    public String getSourceSystemId() {
        return this.sourceSystemId;
    }
        
    public String getUoiid() {
        return this.uoiid;
    }
    
    public void setIsRestricted(String isRestricted) {
        this.isRestricted = isRestricted;
    }
    
    public void setOwningUnitUniqueName(String owningUnitUniqueName) {
        this.owningUnitUniqueName = owningUnitUniqueName;
    }
    
    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }
     
    public void setUoiid(String uoiid) {
        this.uoiid = uoiid;
    }
    
    
    /*  Method :        updateDAMSSourceSystemID
        Arguments:      
        Description:    
        RFeldman 2/2015
    */
    
    public boolean updateDAMSSourceSystemID () {
        int recordsUpdated = 0;

        String sql = "update towner.SI_ASSET_METADATA set source_system_id = '" + getSourceSystemId() + "' " +
                    "where UOI_ID = '" + getUoiid() + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql) ) {
             
            recordsUpdated = pStmt.executeUpdate();
              
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
            if (recordsUpdated != 1) {
                throw new Exception();
            }
            return true;
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error updating source_system_id ", e);
            return false;
        }
             
    }
    
    public boolean populateMetaDataDBLengths () {

        metaDataDBLengths = new HashMap<> ();  
        String sql = "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = 'SI_ASSET_METADATA' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type != 'DATE' " + 
                     "AND column_name NOT IN ('UOI_ID','OWNING_UNIT_UNIQUE_NAME')";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs != null && rs.next()) {
                metaDataDBLengths.put(rs.getString(1),rs.getInt(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain data field lengths ", e);
        
        }
        return true;
    }
    
    public boolean populateIsRestricted () {
  
        String sql = "SELECT is_restricted FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setIsRestricted (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    public boolean populateOwningUnitUniqueName () {
  
        String sql = "SELECT owning_unit_unique_name FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setOwningUnitUniqueName (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    public boolean updatePublicUse () {
  
        int recordsUpdated;
        String sql = "UPDATE towner.si_asset_metadata " +
                    "SET public_use = 'Yes' " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);    
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
       
            recordsUpdated = pStmt.executeUpdate();
            
            if (recordsUpdated == 1) {
                return true;
            }
            else {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update Public Use flag for uoi_id: " + getUoiid() + " " + e);
                return false;
        }
    }
}
