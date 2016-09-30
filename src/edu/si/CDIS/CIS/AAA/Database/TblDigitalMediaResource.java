/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.AAA.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TblDigitalMediaResource {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer collectionId;
    private Integer digitalMediaResourceId;
    private String damsUan;
    
    
    public Integer getCollectionId () {
        return this.collectionId;
    }
    
    public String getDamsUan () {
        return this.damsUan;
    }
    
    public Integer getDigitalMediaResourceId () {
        return this.digitalMediaResourceId;
    }
    
    public void setCollectionId (Integer collectionId) {
        this.collectionId = collectionId;
    }
    
    public void setDamsUan (String damsUan) {
        this.damsUan = damsUan;
    }
      
    public void setDigitalMediaResourceId (Integer digitalResourceId) {
        this.digitalMediaResourceId = digitalResourceId;
    }
    
    public boolean populateCollectionId  () {
        
        String sql =    "SELECT fkCollectionID " +
                        "FROM  dbo.tblDigitalMediaResource " +
                        "WHERE digitalMediaResourceID = " + getDigitalMediaResourceId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
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
    
    public boolean updateDamsUAN () {
        
       int recordsUpdated = 0;
       String sql = "UPDATE dbo.tblDigitalMediaResource " +
                    "SET serviceFileUAN = '" + getDamsUan() + "', " +
                    "damsUANadddate = GETDATE() " +
                    "WHERE digitalMediaResourceID = " + getDigitalMediaResourceId() ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
      try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql) ) {
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in AAA CIS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error, unable to update DamsUAN in AAA CIS", e);
                return false;
        }
        return true;
    }
 
}
