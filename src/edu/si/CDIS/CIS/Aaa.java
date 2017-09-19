/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CIS.AAA.Database.TblCollection;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;

/**
 *
 * @author rfeldman
 */
public class Aaa implements CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap) {
        
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        TblCollection tblCollection = new TblCollection();
        tblCollection.setCollectionId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        tblCollection.populateCollcode();
        
        return "Collection: " + tblCollection.getCollcode();
        
    }
    
    public String returnImageInfoForReport (CDISMap cdisMap) {
        return "";
    }
}
