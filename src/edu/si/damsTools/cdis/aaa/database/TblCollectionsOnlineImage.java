/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.aaa.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TblCollectionsOnlineImage {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer collectionId;
    private Integer collectionsOnlineImageId;
    private String damsUan;

    public String getDamsUan () {
        return this.damsUan;
    }
    
        
    public Integer getCollectionId () {
        return this.collectionId;
    }
        
    public Integer getCollectionsOnlineImageId () {
        return this.collectionsOnlineImageId;
    }

    public void setDamsUan (String damsUan) {
        this.damsUan = damsUan;
    }
        
    public void setCollectionId (Integer collectionId) {
        this.collectionId = collectionId;
    }
        
    public void setCollectionOnlineImageId (Integer collectionsOnlineImageId) {
        this.collectionsOnlineImageId = collectionsOnlineImageId;
    }
     
    public boolean populateCollectionId  () {
        
        String sql =    "SELECT fkCollectionID " +
                        "FROM  dbo.tblCollectionsOnlineImage " +
                        "WHERE collectionsOnlineImageId = " + getCollectionsOnlineImageId();
        
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
    
    public boolean updateDamsUAN () {
        
       int recordsUpdated = 0;
       String sql = "UPDATE tblCollectionsOnlineImage " +
                    "SET damsUAN = '" + getDamsUan() + "', " +
                    "damsUANadddate = GETDATE() " +
                    "WHERE collectionsOnlineImageId = " + getCollectionsOnlineImageId() ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
      try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql) ) {
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in AAA CIS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error, unable to update DamsUAN in AAA CIS", e);
                return false;
        }
        return true;
    }
    
}
