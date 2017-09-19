/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.Database.CDISRefIdMap;
import edu.si.CDIS.Database.CDISMap;

/**
 *
 * @author rfeldman
 */
public class Aspace implements CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap) {
        
        CDISRefIdMap cdisRefidMap = new CDISRefIdMap();
        cdisRefidMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisRefidMap.populateRefIdFromMapId();
                        
        return "CDISRefId: " +  cdisRefidMap.getRefId();
    }
    
    public String returnImageInfoForReport (CDISMap cdisMap) {
        return "";
    }
    
}
