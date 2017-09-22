/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.cdis.database.CDISMap;

/**
 *
 * @author rfeldman
 */
public interface CisAttr {
    
    public String returnGrpInfoForReport (CDISMap cdisMap);
    
    public String returnImageInfoForReport (CDISMap cdisMap);
    
}
