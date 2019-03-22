/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.identifier;

import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisMap;

/**
 *
 * @author rfeldman
 */
public class Uan implements IdentifierType {
    
    private String uan;
    
    public String getIdentifierValue() {
        return uan;
    }
    
    public void setIdentifierValue(String identifierValue) {
        uan = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }
}
