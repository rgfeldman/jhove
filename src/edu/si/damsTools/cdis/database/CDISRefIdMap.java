/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

import edu.si.damsTools.DamsTools;
import java.util.ArrayList;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISRefIdMap {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer cdisMapId;
    private Integer cdisRefIdMapId;
    private String refId;
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
    
    public Integer getCdisRefIdMapId () {
        return this.cdisRefIdMapId;
    }
    
    public String getRefId() {
        return this.refId;
    }
    
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
    }
    
    public void setCdisRefIdMapId (Integer cdisRefIdMapId) {
        this.cdisRefIdMapId = cdisRefIdMapId;
    }
        
    public void setRefId (String refId) {
        this.refId = refId;
    }
    
    
    public boolean createRecord() {
             
        String sql = "INSERT INTO cdis_ref_Id_map (" +
                        "cdis_ref_id_map_id, " +
                        "cdis_map_id, " +
                        "ref_id )" +
                    "VALUES (" +
                        "cdis_ref_id_map_id_seq.NextVal, " +
                        getCdisMapId() + ", " +
                        "'" + getRefId() + "')";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_REF_ID_MAP table", e );
                return false;
        }      
        
        return true;
    }
    
    public boolean populateRefIdFromMapId () {
        String sql = "SELECT    ref_id " +
                     "FROM      cdis_ref_Id_map " +
                     "WHERE     cdis_map_id = " + getCdisMapId();
                
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setRefId (rs.getString(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain cdisMediaType ", e );
                return false;
        }
        return true;
    }
    
    public ArrayList<Integer> returnCdisMapIdsForRefId() {

        ArrayList cdisMapIdsForRefId = new ArrayList<>();
        
        String sql = "SELECT    cdis_map_id " +
                     "FROM      cdis_ref_id_map " +
                     "WHERE     ref_id = '" + getRefId() + "'";
        
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
    
}
