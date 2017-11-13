/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.aaa.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TblDigitalMediaResource {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String accessFileUan;
    private Integer collectionId;
    private Integer digitalMediaResourceId;
    private String masterFileUan;
    private String serviceFileUan;
    
    
    public String getAccessFileUan () {
        return this.accessFileUan == null ? "" : this.accessFileUan;
    }
    
    public Integer getCollectionId () {
        return this.collectionId;
    }
    
    public String getMasterFileUan () {
        return this.masterFileUan == null ? "" : this.masterFileUan;
    }
      
    public String getServiceFileUan () {
        return this.serviceFileUan;
    }
    
    public Integer getDigitalMediaResourceId () {
        return this.digitalMediaResourceId;
    }
    
    public void setAccessFileUan (String accessFileUan) {
        this.accessFileUan = accessFileUan;
    }
      
    public void setCollectionId (Integer collectionId) {
        this.collectionId = collectionId;
    }
    
    public void setMasterFileUan (String masterFileUan) {
        this.masterFileUan = masterFileUan;
    }
        
    public void setServiceFileUan (String serviceFileUan) {
        this.serviceFileUan = serviceFileUan;
    }
      
    public void setDigitalMediaResourceId (Integer digitalResourceId) {
        this.digitalMediaResourceId = digitalResourceId;
    }
    
    public boolean populateCollectionId  () {
        
        String sql =    "SELECT fkCollectionID " +
                        "FROM  dbo.tblDigitalMediaResource " +
                        "WHERE digitalMediaResourceID = " + getDigitalMediaResourceId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setCollectionId(rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain CollectionID for digResource", e );
                return false;
        }
        return true;
    }
    
 
}
