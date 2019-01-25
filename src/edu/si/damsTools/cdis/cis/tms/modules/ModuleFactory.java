/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.modules;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class ModuleFactory {
    
    public ModuleType moduleChooser() {
        
        ModuleType moduleType = null;
        
        switch (DamsTools.getSubOperation()) {
            case "constituent" :
                moduleType = new Constituent();
                break;
            case "exhibition" :
                moduleType = new Exhibition();
                break;         
            case "object" :
                moduleType = new Object();
                break;
        }       
        return moduleType;
    }
}
