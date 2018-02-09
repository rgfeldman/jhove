/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.applications;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.dams.DamsRecord;

/**
 *
 * @author rfeldman
 */
public interface CisRecordAttr {
    
    public String returnGrpInfoForReport (CdisMap cdisMap);
    
    public String getCisImageIdentifier();
    
    public String getGroupIdentifier ();
    
    public boolean setBasicValues(String identifier,  DamsRecord damsRecord);
    
    public String returnCdisGroupType();
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap);
    
    public String returnCisUpdateCode();
 
}
