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
public class OralHistoryMediaResourceId implements IdentifierType {
    
    private String digitalMediaResourceId;
    
    public String getIdentifierValue() {
        return digitalMediaResourceId;
    }
    
    public void setIdentifierValue(String identifierValue) {
        digitalMediaResourceId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        return true;
    }

}
