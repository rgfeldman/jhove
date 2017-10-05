/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.aaa.database.TblCollection;
import edu.si.damsTools.cdis.aaa.database.TblCollectionsOnlineImage;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
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
    
    public boolean setBasicValues (String cisRecordId) {
        imageMediaRecord.setCollectionOnlineImageId(Integer.parseInt(cisRecordId));
        imageMediaRecord.populateCollectionId();
        //get the objectID

        return true;
    }
    
    
    public String getCisImageIdentifier () {
        return this.imageMediaRecord.getCollectionsOnlineImageId().toString();
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

}
