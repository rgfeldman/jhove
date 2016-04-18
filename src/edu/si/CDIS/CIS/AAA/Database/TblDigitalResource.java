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
public class TblDigitalResource {
    
    private Integer collectionId;
    private Integer digitalResourceId;
    
    
   public Integer getCollectionId () {
        return this.collectionId;
    }
    
    public Integer getDigitalResourceId () {
        return this.digitalResourceId;
    }
        
    public void setCollectionId (Integer collectionId) {
        this.collectionId = collectionId;
    }
      
    public void setDigitalResourceId (Integer digitalResourceId) {
        this.digitalResourceId = digitalResourceId;
    }
     
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public boolean populateCollectionId () {
        
        String sql =    "SELECT fkCollectionDigResId " +
                        "FROM  dbo.tblDigitalResource " +
                        "WHERE fkDigitalResourceID = " + getDigitalResourceId();
        
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
    
}
