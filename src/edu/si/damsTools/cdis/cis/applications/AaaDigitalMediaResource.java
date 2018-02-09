/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.applications;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.aaa.database.TblCollection;
import edu.si.damsTools.cdis.cis.aaa.database.TblDigitalMediaResource;
import edu.si.damsTools.cdis.dams.DamsRecord;
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
public class AaaDigitalMediaResource implements CisRecordAttr{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final TblDigitalMediaResource tblDigitalMediaResource;
    
    public AaaDigitalMediaResource () {
        tblDigitalMediaResource = new TblDigitalMediaResource();
    }
    
    public String getCisImageIdentifier () { 
        return tblDigitalMediaResource.getDigitalMediaResourceId().toString();
    }
    
    public String getGroupIdentifier () {
        return tblDigitalMediaResource.getCollectionId().toString();
    }
    
    public boolean setBasicValues (String identifier,  DamsRecord damsRecord) {
        tblDigitalMediaResource.setDigitalMediaResourceId(Integer.parseInt(identifier));
        tblDigitalMediaResource.populateCollectionId();
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
        return "return linkCisRecord";
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }
    
    public String returnCisUpdateCode() {
        return "CPS";
    } 
    
}
