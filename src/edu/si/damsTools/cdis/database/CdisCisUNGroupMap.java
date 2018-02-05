/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

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
public class CdisCisUNGroupMap {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer cdisCisUNGroupMapId;
    private Integer cdisMapId;
    private String cisGroupCd;
    private String cisName;
    private String cisGroupValue;
    
    public Integer getCdisMapId() {
        return this.cdisMapId;
    }
    
    public String getCisGroupCd() {
        return this.cisGroupCd;
    }
    
    public String getCisName() {
        return this.cisName;
    }
    
    public String getCisGroupValue() {
        return this.cisGroupValue;
    }    
        
    public Integer getCdisCisGroupMapId() {
        return this.cdisCisUNGroupMapId;
    }    
    
    public void setCdisMapId(Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCisGroupCd(String cisGroupCd) {
        this.cisGroupCd = cisGroupCd;
    }
        
    public void setCisGroupValue(String cisGroupValue) {
        this.cisGroupValue = cisGroupValue;
    }    
    
    public boolean createRecord() {
        
         String sql = "INSERT INTO cdis_cis_un_group_map (" +
                        "cdis_cis_un_group_map_id, " +
                        "cdis_map_id, " +
                        "cis_name, " +
                        "cis_group_cd, " +
                        "cis_group_value )" +
                    "VALUES (" +
                        "cdis_cis_un_group_map_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + DamsTools.getProperty("cis") + "', " +
                        "'" + getCisGroupCd() + "', " +
                        "'" + getCisGroupValue () + "')";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into cdis_un_group_map table", e );
                return false;
        }      
        
        return true;
    }
    
     public boolean populateCisGroupValueForCdisMapIdType() {
        String sql = "SELECT cis_group_value " +
                     "FROM cdis_cis_un_group_map " +
                     "WHERE cdis_map_id = " + getCdisMapId() +
                     " AND cis_group_cd = " + "'" + this.cisGroupCd +"'";

        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.cisGroupValue = rs.getString(1);
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_cis_group_map_id ", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIdForMapIDGroupCdCis() {
        String sql = "SELECT cdis_cis_un_group_map_id " +
                     "FROM cdis_cis_un_group_map " +
                     "WHERE cdis_map_id = " + getCdisMapId() + 
                     " AND cis_group_cd = '" + getCisGroupCd() + "' " +
                     "AND cis_name = '" + DamsTools.getProperty("cis") + "'";

        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.cdisCisUNGroupMapId = rs.getInt(1);
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_cis_group_map_id ", e );
                return false;
        }
        return true;
    }
    
    public ArrayList<Integer> returnCdisMapIdsForCdValue() {

        ArrayList cdisMapIdsForRefId = new ArrayList<>();
        
        String sql = "SELECT    cdis_map_id " +
                     "FROM      cdis_cis_un_group_map " +
                     "WHERE     cis_group_cd = '" + this.cisGroupCd + "' " +
                     "AND       cis_group_value = '" + this.cisGroupValue + "'";  
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            while (rs.next()) {
                cdisMapIdsForRefId.add(rs.getInt(1));
            }   
            
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain mapid list from refId ", e );
                return null;
        }
        
        return cdisMapIdsForRefId;
        
    }
    
    public boolean updateCisGroupValue() {
        int rowsUpdated = 0;
        String sql =  "UPDATE cdis_cis_un_group_map " +
                      "SET cis_group_value = '" + getCisGroupValue() + "' " +
                      "WHERE cdis_cis_un_group_map_id = " + getCdisCisGroupMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            
            rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with cis_unique_media_id", e );
                return false;
        } 
        return true;
    }

}
