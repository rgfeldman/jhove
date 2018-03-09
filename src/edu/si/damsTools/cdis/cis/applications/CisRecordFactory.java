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
            case "aaa" :
                if (DamsTools.getProperty("mediaTypeConfigId").equals("16")) {
                    cisRecord = new AaaCollectionImage();
                }
                break;
            case "aspace" :
                cisRecord = new Aspace();
                break;
            case "cdisDb" :
                cisRecord = new CdisDb();
                break;
            case "iris" :
                cisRecord = new IrisBg();
                break;
            case "tms" :
                cisRecord = new Tms();
                break;
        }
        
        return cisRecord;
    }
}
