/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CDISRefIdMap;
import edu.si.damsTools.cdis.database.CdisMap;

/**
 *
 * @author rfeldman
 */
public class Aspace implements CisRecordAttr {
    
    private String cisImageIdentifier;
    private String cisGroupIdentifier; 
    
    public void setUniqueImageIdentifier (String identifier) {
        this.cisImageIdentifier = identifier;
    }
    
    public String getCisImageIdentifier () {
        return this.cisImageIdentifier;
    }
    
    public String getGroupIdentifier () {
        return this.cisGroupIdentifier;
    }
    
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CDISRefIdMap cdisRefidMap = new CDISRefIdMap();
        cdisRefidMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisRefidMap.populateRefIdFromMapId();
                        
        return "CDISRefId: " +  cdisRefidMap.getRefId();
    }
    
    public boolean populateGroupIdForImageId() {
        return false;
    }
    
    
}
