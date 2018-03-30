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
public class CdisCisIdentifierMap {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer cdisCisIdentifierMapId;
    private Integer cdisMapId;
    private String cisIdentifierCd;
    private String cisName;
    private String cisIdentifierValue;
    
    public Integer getCdisMapId() {
        return this.cdisMapId;
    }
    
    public String getCisIdentifierCd() {
        return this.cisIdentifierCd;
    }
    
    public String getCisName() {
        return this.cisName;
    }
    
    public String getCisIdentifierValue() {
        return this.cisIdentifierValue;
    }    
        
    public Integer getCdisCisIdentifierMapId() {
        return this.cdisCisIdentifierMapId;
    }    
    
    public void setCdisMapId(Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCisIdentifierCd(String cisIdentifierCd) {
        this.cisIdentifierCd = cisIdentifierCd;
    }
        
    public void setCisIdentifierValue(String cisIdentifierValue) {
        this.cisIdentifierValue = cisIdentifierValue;
    }    
    
    public boolean createRecord() {
        
         String sql = "INSERT INTO cdis_cis_identifier_map (" +
                        "cdis_cis_identifier_map_id, " +
                        "cdis_map_id, " +
                        "cis_name, " +
                        "cis_identifier_cd, " +
                        "cis_identifier_value )" +
                    "VALUES (" +
                        "cdis_cis_identifier_map_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + DamsTools.getProperty("cis") + "', " +
                        "'" + getCisIdentifierCd() + "', " +
                        "'" + getCisIdentifierValue () + "')";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into cdis_cis_identifier_map table", e );
                return false;
        }      
        
        return true;
    }
    
     public boolean populateCisIdentifierValueForCdisMapIdType() {
        String sql = "SELECT cis_identifier_value " +
                     "FROM cdis_cis_identifier_map " +
                     "WHERE cdis_map_id = " + getCdisMapId() +
                     " AND cis_identifier_cd = " + "'" + this.cisIdentifierCd +"'";

        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.cisIdentifierValue = rs.getString(1);
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cis_identifier_value ", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIdForMapIDIdentifierCdCis() {
        String sql = "SELECT cdis_cis_identifier_map_id " +
                     "FROM cdis_cis_identifier_map " +
                     "WHERE cdis_map_id = " + getCdisMapId() + 
                     " AND cis_identifier_cd = '" + getCisIdentifierCd() + "' " +
                     "AND cis_name = '" + DamsTools.getProperty("cis") + "'";

        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.cdisCisIdentifierMapId = rs.getInt(1);
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdis_cis_identifier_map_id ", e );
                return false;
        }
        return true;
    }
    
    public ArrayList<Integer> returnCdisMapIdsForCisCdValue() {

        ArrayList cdisMapIdsForRefId = new ArrayList<>();
        
        String sql = "SELECT    cdm.cdis_map_id " +
                     "FROM      cdis_cis_identifier_map ccim " +
                     "INNER JOIN cdis_map cdm " +
                     "ON        cdm.cdis_map_id = ccim.cdis_map_id " +
                     "WHERE     ccim.cis_identifier_cd = '" + this.cisIdentifierCd + "' " +
                     "AND       ccim.cis_name = '" + DamsTools.getProperty("cis") + "' " +
                     "AND       ccim.cis_identifier_value = '" + this.cisIdentifierValue + "'" +
                     "AND       cdm.project_cd = '"  + DamsTools.getProperty("projectCd") +"'" ;
        
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
    
    public boolean updateCisIdentifierValue() {
        int rowsUpdated = 0;
        String sql =  "UPDATE cdis_cis_identifier_map " +
                      "SET cis_identifier_value = '" + getCisIdentifierValue() + "' " +
                      "WHERE cdis_cis_identifier_map_id = " + getCdisCisIdentifierMapId();
        
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
