/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CdisRefIdMap;
import edu.si.damsTools.cdis.database.CdisMap;

/**
 *
 * @author rfeldman
 */
public class Aspace implements CisRecordAttr {
    
    
    public String getCisImageIdentifier () {
        return null;
    }
    
    public String getGroupIdentifier () {
        return null;
    }
    
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisRefIdMap cdisRefidMap = new CdisRefIdMap();
        cdisRefidMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisRefidMap.populateRefIdFromMapId();
                        
        return "CDISRefId: " +  cdisRefidMap.getRefId();
    }
    
    public boolean setBasicValues (String cisRecordId) {

        return true;
    }

    public String returnCdisGroupType() {
        return null;
    }
    
}
