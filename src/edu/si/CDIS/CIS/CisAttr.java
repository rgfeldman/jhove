/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.Database.CDISMap;

/**
 *
 * @author rfeldman
 */
public interface CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap);
    
    public String returnImageInfoForReport (CDISMap cdisMap);
    
}
