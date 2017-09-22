/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CDISMap;

/**
 *
 * @author rfeldman
 */
public class CisFactory {
    
    public CisFactory() {
        
    }
    
    public CisAttr cisChooser() {
        
        CisAttr cisAttr = null;
        
        switch (DamsTools.getProperty("cis")) {
            case "aaa" :
                cisAttr = new Aaa();
                break;
            case "aSpace" :
                cisAttr = new Aspace();
                break;
            case "iris" :
                cisAttr = new IrisBg();
                break;
            case "tms" :
                cisAttr = new Tms();
                break;
        }
        
        return cisAttr;
    }
}
