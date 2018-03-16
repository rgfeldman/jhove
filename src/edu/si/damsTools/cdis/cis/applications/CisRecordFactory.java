/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.applications;

import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class CisRecordFactory {
    
    public CisRecordFactory() {
        
    }
    
    public CisRecordAttr cisChooser() {
     
        CisRecordAttr cisRecord = null;
        
        switch (DamsTools.getProperty("cis")) {
            case "aspace" :
                cisRecord = new Aspace();
                break;
            case "cdisDb" :
                cisRecord = new CdisDb();
                break;
        }
        
        return cisRecord;
    }
}
