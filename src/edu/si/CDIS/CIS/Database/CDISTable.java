/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import edu.si.CDIS.utilties.DataProvider;
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
    
    // Update CDIS table to log this transaction
    public int updateIDSSyncDate(CDISTable cdisTbl, Connection tmsConn) {
        
        int updateCount = 0;
        
        String sql = "update CDIS " +
                    "set SyncIDSPathDate = SYSDATETIME() " +
                    "where RenditionID = " + cdisTbl.getRenditionId();

        logger.log(Level.FINEST,"SQL! " + sql);

        updateCount = DataProvider.executeUpdate(tmsConn, sql);

        return (updateCount);

    }
    
    public boolean createRecord(CDISTable cdisTbl, Connection tmsConn) {
        
        boolean inserted = false;
        
        // Get the ObjectID if it exists 

        String sql = "Insert into CDIS (RenditionID, RenditionNumber, ObjectID, UAN, UOIID, LinkDate) " +
                    "values ( " + cdisTbl.getRenditionId() + ", '" +
                    cdisTbl.getRenditionNumber() + "', " +
                    cdisTbl.getObjectId() + ", '" +
                    cdisTbl.getUAN() + "', '" +
                    cdisTbl.getUOIID() + "', " +
                    "GETDATE() )";
        
        logger.log(Level.FINEST,"SQL! " + sql);
   
        inserted = DataProvider.executeInsert(tmsConn, sql);     
                
        return inserted;
    }
        
}
