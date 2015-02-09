/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned.SourceDatabase;

import java.sql.Connection;
import edu.si.data.DataProvider;

/**
 *
 * @author rfeldman
 */
public class CDISTable {
    
    // attributes
    Integer CDIS_ID;
    String metaDataSyncDate;
    Integer objectID;
    String IDSRestrict;
    Integer renditionId;
    String renditionNumber;
    String UOIID;
    
       
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
    
    public void setUOIID (String UOIID) {
        this.UOIID = UOIID;
    }
        
}
