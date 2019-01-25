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
public interface ModuleType {
    
    public boolean populateRecordId(DamsRecord damsRecord);
    
    public Integer returnRecordId();
    
    public String returnMappedMethod();
    
    public String returnTableId();
    
}
