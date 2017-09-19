/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CIS.TMS.Database.Objects;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;


/**
 *
 * @author rfeldman
 */
public class Tms implements CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap) {
        
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        Objects objects = new Objects();
        objects.setObjectId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        objects.populateObjectNumberForObjectID();
                        
        return "Object: " + objects.getObjectNumber() ;
    }
    
    public String returnImageInfoForReport (CDISMap cdisMap) {
        return "";
    }
}
