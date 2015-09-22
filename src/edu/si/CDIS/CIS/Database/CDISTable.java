/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import edu.si.CDIS.utilties.DataProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISTable {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    // attributes
    Integer CDIS_ID;
    String metaDataSyncDate;
    Integer objectID;
    String IDSRestrict;
    Integer renditionId;
    String renditionNumber;
    String UOIID;
    String UAN;
    
       
    // get methods
    public int getCDIS_ID () {
        return this.CDIS_ID;
    }
    
    public String getIDSRestrict () {
        return this.IDSRestrict;
    }
    
    public int getObjectId () {
        return this.objectID;
    }
    
    public int getRenditionId () {
        return this.renditionId;
    }
      
    public String getRenditionNumber () {
        return this.renditionNumber;
    }
    
    public String getMetaDataSyncDate () {
        return this.metaDataSyncDate;
    }
    
    public String getUAN () {
        return this.UAN;
    }
    
    public String getUOIID () {
        return this.UOIID;
    }

    
    //set methods
    public void setCDIS_ID (int CDIS_ID) {
        this.CDIS_ID = CDIS_ID;
    }      
    
    public void setIDSRestrict (String IDSRestrict) {
        this.IDSRestrict = IDSRestrict;
    }
    
    public void setMetaDataSyncDate (String metaDataSyncDate) {
        this.metaDataSyncDate = metaDataSyncDate;
    }
    
    public void setObjectId (int objectID) {
        this.objectID = objectID;
    }
    
    public void setRenditionId (int renditionId) {
        this.renditionId = renditionId;
    }
    
    public void setRenditionNumber (String renditionNumber) {
        this.renditionNumber = renditionNumber;
    }
    
    public void setUAN (String UAN) {
        this.UAN = UAN;
    }
    
    public void setUOIID (String UOIID) {
        this.UOIID = UOIID;
    }
 
    public boolean createRecord(CDISTable cdisTbl, Connection cisConn) {
        
        Statement stmt = null;
        ResultSet rs = null;
        
        String sql = "Insert into CDIS (RenditionID, RenditionNumber, ObjectID, UAN, UOIID, LinkDate) " +
                    "values ( " + cdisTbl.getRenditionId() + ", '" +
                    cdisTbl.getRenditionNumber() + "', " +
                    cdisTbl.getObjectId() + ", '" +
                    cdisTbl.getUAN() + "', '" +
                    cdisTbl.getUOIID() + "', " +
                    "GETDATE() )";
        
        logger.log(Level.FINEST,"SQL! " + sql);  
        
        try {
            stmt = cisConn.createStatement();
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.CDIS_ID = rs.getInt(1);
            }    
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
                
        return true;
    }
    
    // Update CDIS table to log this transaction
    public int updateIDSSyncDate(Connection cisConn) {
        
        int updateCount = 0;
        
        String sql = "update CDIS " +
                    "set SyncIDSPathDate = SYSDATETIME() " +
                    "where CDIS_ID = " + getCDIS_ID();

        logger.log(Level.FINEST,"SQL! " + sql);

        updateCount = DataProvider.executeUpdate(cisConn, sql);

        return (updateCount);

    }
    
    public int updateThumbnailSyncDate(Connection cisConn) {
        
        int updateCount = 0;
        
        String sql = "update CDIS " +
                    "set ThumbnailSyncDate = SYSDATETIME() " +
                    "where RenditionID = " + getRenditionId();

        logger.log(Level.FINEST,"SQL! " + sql);

        updateCount = DataProvider.executeUpdate(cisConn, sql);

        return (updateCount);

    }
        
}
