/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.modules;

import edu.si.damsTools.cdis.dams.DamsRecord;

/**
 *
 * @author rfeldman
 */
public class Constituent implements ModuleType {
    
    private Integer constituentId;
    
    public boolean populateRecordId(DamsRecord damsRecord) {
        return true;
    }
    
    public Integer returnRecordId() {
        return this.constituentId;
    }
    
    public String returnMappedMethod() {
        return null;
    }
    
    public String returnTableId() {
        return "23";
    }
}
