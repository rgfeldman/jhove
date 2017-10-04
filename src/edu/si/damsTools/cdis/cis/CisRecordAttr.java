/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CdisMap;

/**
 *
 * @author rfeldman
 */
public interface CisRecordAttr {
    
    public boolean populateGroupIdForImageId();
    
    public String returnGrpInfoForReport (CdisMap cdisMap);
    
    public void setUniqueImageIdentifier(String identifier);
    
    public String getCisImageIdentifier();
    
    public String getGroupIdentifier ();
 
}
