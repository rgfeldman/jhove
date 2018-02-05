/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.cis.iris.database.SI_IrisDAMSMetaCore;
import edu.si.damsTools.cdis.dams.DamsRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class IrisBg implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final SI_IrisDAMSMetaCore sI_IrisDAMSMetaCore;
    
    public IrisBg() {
        sI_IrisDAMSMetaCore = new SI_IrisDAMSMetaCore();
    }
    
    public String getCisImageIdentifier () {
        return sI_IrisDAMSMetaCore.getImageLibId();
    }

    public String getGroupIdentifier () {
        return sI_IrisDAMSMetaCore.getItemAccnoFull();
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        return "Accno: " + cdisObjectMap.getCisUniqueObjectId();    
    }
    
     public boolean setBasicValues (String identifier, String identifierType) {
         sI_IrisDAMSMetaCore.setImageLibId(identifier);
         sI_IrisDAMSMetaCore.populateItemAccnoFull();
         return true;
    }
     
    public String returnCdisGroupType() {
        return "cdisObjectMap";
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }
    
    public String returnCisUpdateCode() {
        return null;
    } 
        
}
