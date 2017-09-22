/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdis.database.CDISObjectMap;

/**
 *
 * @author rfeldman
 */
public class IrisBg implements CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap) {
        
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        return "Accno: " + cdisObjectMap.getCisUniqueObjectId();    
    }
    
    public String returnImageInfoForReport (CDISMap cdisMap) {
        return "";
    }
        
}
