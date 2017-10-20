/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisDb implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    String cisIdentifier;
    
    public CdisDb() {
        
    }
    
    public boolean setBasicValues (String identifier) {
        cisIdentifier = identifier;
        return true;
    }
    
    
    public String getCisImageIdentifier () {
        return this.cisIdentifier;
    }
    
    public String getGroupIdentifier() {
        return null;
    }
    
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {   
        return null;  
    }

    public String returnCdisGroupType() {
        return null;
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord) {
        return true;
    }
}
