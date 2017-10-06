/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class Emu implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    String irn;
    
    public Emu() {
        
    }
    
    public boolean setBasicValues (String cisRecordId) {
        irn = cisRecordId;
        return true;
    }
    
    
    public String getCisImageIdentifier () {
        return this.irn;
    }
    
    public String getGroupIdentifier() {
        return null;
    }
    
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {   
        return null;  
    }

    public String returnCdisGroupType() {
        return "cdisObjectMap";
    }
}
