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
public class ControlRecordId implements IdentifierType {
    
    private String hcr;
    
    public String getIdentifierCd() {
        return "hcr";
    }
    
    public String getIdentifierValue() {
        return hcr;
    }
    
    public void setIdentifierValue(String identifierValue) {
        hcr = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }
    
}
