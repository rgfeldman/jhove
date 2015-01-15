/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned.SourceDatabase;

/**
 *
 * @author rfeldman
 */
public class CDISTable {
    
    // attributes
    Integer CDIS_ID;
    Integer RenditionID;
    String UOIID;
    String RenditionNumber;
    String MetaDataSyncDate;

    
    // get methods
    public int getCDIS_ID () {
        return this.CDIS_ID;
    }
    
    public int getRenditionID () {
        return this.RenditionID;
    }
    
    public String getUOIID () {
        return this.UOIID;
    }
    
    public String getRenditionNumber () {
        return this.RenditionNumber;
    }
    
    public String getMetaDataSyncDate () {
        return this.MetaDataSyncDate;
    }

    //set methods
    public void setCDIS_ID (int CDIS_ID) {
        this.CDIS_ID = CDIS_ID;
    }      
    
    public void setRenditionID (int renditionId) {
        this.RenditionID = renditionId;
    }
    
}
