/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams.database;

import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;


public class SiAssetMetadata {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    //class attributes
    private String eadRefId;
    private String isRestricted;
    private Integer maxIdsSize;
    private String owningUnitUniqueName;
    private String sourceSystemId;
    private String uoiid;  
    private String holdingUnit;

    public String getEadRefId () {
        return this.eadRefId;
    }
    
    public String getIsRestricted() {
        return this.isRestricted == null ? "" : this.isRestricted;
    }
    
    public String getHoldingUnit() {
        return this.holdingUnit;
    }
    
    public Integer getMaxIdsSize() {
        return this.maxIdsSize;
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
    
    public void setEadRefId(String eadRefId) {
        this.eadRefId = eadRefId;
    }
    
    public void setHoldingUnit(String holdingUnit) {
        this.holdingUnit = holdingUnit;
    }
        
    public void setIsRestricted(String isRestricted) {
        this.isRestricted = isRestricted;
    }
    
    public void setMaxIdsSize(Integer maxIdsSize) {
        this.maxIdsSize = maxIdsSize;
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
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {
             
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
    
    public boolean populateEadRefId () {
        
        String sql = "SELECT ead_ref_id " +
                    "FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setEadRefId(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain EadRefId from cdis_map", e );
                return false;
        }
        return true;
    }
    
    public boolean populateSiAsstData () {
        
        String sql = "SELECT holding_unit, owning_unit_unique_name, is_restricted, max_ids_size, source_system_id, ead_ref_id " +
                    "FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setHoldingUnit (rs.getString(1));
                setOwningUnitUniqueName (rs.getString(2));
                setIsRestricted (rs.getString(3));
                setMaxIdsSize (rs.getInt(4));
                setSourceSystemId(rs.getString(5));
                setEadRefId(rs.getString(6));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain basic data from siAsstMetadata", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIsRestricted () {
  
        String sql = "SELECT is_restricted FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setIsRestricted (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain restricted from siAsstMetadata", e );
                return false;
        }
        return true;
    }
    
    public boolean populateOwningUnitUniqueName () {
  
        String sql = "SELECT owning_unit_unique_name FROM towner.si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
       
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
