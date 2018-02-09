/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.applications;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.aaa.database.TblCollection;
import edu.si.damsTools.cdis.cis.aaa.database.TblCollectionsOnlineImage;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.dams.DamsRecord;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class AaaCollectionImage implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final TblCollectionsOnlineImage imageMediaRecord;
    
    public AaaCollectionImage() {
        imageMediaRecord = new TblCollectionsOnlineImage();
    }
    
    public boolean setBasicValues (String identifier, DamsRecord damsRecord) {
        imageMediaRecord.setCollectionOnlineImageId(Integer.parseInt(identifier));
        imageMediaRecord.populateCollectionId();
        return true;
    }
    
    
    public String getCisImageIdentifier () {
        return this.imageMediaRecord.getCollectionsOnlineImageId().toString();
    }
    
    public String getIdentifierType() {
        return null;
    }
    
    public String getGroupIdentifier() {
        return this.imageMediaRecord.getCollectionId().toString();
    }
    
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        TblCollection tblCollection = new TblCollection();
        tblCollection.setCollectionId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        tblCollection.populateCollcode();
        
        return "Collection: " + tblCollection.getCollcode();
        
    }

    public String returnCdisGroupType() {
        return "cdisObjectMap";
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }
    
    public String returnCisUpdateCode() {
        return "CPS";
    } 
}
