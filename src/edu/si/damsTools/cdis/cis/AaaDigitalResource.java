/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.aaa.database.TblCollection;
import edu.si.damsTools.cdis.aaa.database.TblDigitalResource;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class AaaDigitalResource implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final TblDigitalResource tblDigitalResource;
    
    public AaaDigitalResource() {
        tblDigitalResource = new TblDigitalResource();
    }
    
    public String getCisImageIdentifier () {
        return this.tblDigitalResource.getDigitalResourceId().toString();
    }
    
    public String getGroupIdentifier () {
        return this.tblDigitalResource.getCollectionId().toString();
    }
    
    public boolean setBasicValues (String cisRecordId) {

        return true;
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
}
