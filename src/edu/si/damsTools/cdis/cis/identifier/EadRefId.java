/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.identifier;

/**
 *
 * @author rfeldman
 */
public class EadRefId implements IdentifierType {
    
    private String eadRefId;
    
    public String getIdentifierCd() {
        return "ead";
    }
    
    public String getIdentifierValue() {
        return eadRefId;
    }
    
    public void setIdentifierValue(String identifierValue) {
        eadRefId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return true;
    }
        
}
