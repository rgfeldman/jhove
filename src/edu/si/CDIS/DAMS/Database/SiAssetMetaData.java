/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.SQLException;
import java.util.logging.Level;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;
import java.util.HashMap;



public class SiAssetMetaData {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    //class attributes
    private String owningUnitUniqueName;
    private String uoiid;  
    
    public HashMap <String,Integer> metaDataDBLengths; 
      
    public String getOwningUnitUniqueName() {
           return this.owningUnitUniqueName;
    }
    
    public String getUoiid() {
           return this.uoiid;
    }
    
    public void setOwningUnitUniqueName(String owningUnitUniqueName) {
           this.owningUnitUniqueName = owningUnitUniqueName;
    }
    
     
    public void setUoiid(String uoiid) {
           this.uoiid = uoiid;
    }
    
    
    /*  Method :        updateDAMSSourceSystemID
        Arguments:      
        Description:    
        RFeldman 2/2015
    */
    
    public int updateDAMSSourceSystemID (String uoiid, String sourceSystemId) {
        int recordsUpdated = 0;

        String sql = "update SI_ASSET_METADATA set source_system_id = '" + sourceSystemId + "' " +
                    "where UOI_ID = '" + uoiid + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql) ) {
             
            recordsUpdated = pStmt.executeUpdate(sql);
              
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }
            
        return recordsUpdated;    
    }
    
    public boolean populateMetaDataDBLengths () {

        metaDataDBLengths = new HashMap<> ();  
        String sql = "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = 'SI_ASSET_METADATA' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type != 'DATE' " + 
                     "AND column_name NOT IN ('PUBLIC_USE','UOI_ID','OWNING_UNIT_UNIQUE_NAME')";
        
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
    
    public boolean populateOwningUnitUniqueName () {
  
        String sql = "SELECT owning_unit_unique_name FROM si_asset_metadata " +
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
        String sql = "UPDATE si_asset_metadata " +
                    "SET public_use = 'Y' " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);    
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
       
            recordsUpdated = pStmt.executeUpdate(sql);
            
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
