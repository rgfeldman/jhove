/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;

/**
 *
 * @author rfeldman
 */
public class CisFactory {
    
    public CisFactory() {
        
    }
    
    public CisAttr cisChooser() {
        
        CisAttr cisAttr = null;
        
        switch (CDIS.getProperty("cis")) {
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
