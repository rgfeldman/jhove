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
public class AccessionNo implements IdentifierType {
    
    private String accessionNo;
    
    public String returnIdentifierCd() {
        return "acc";
    }
    
    public void setIdentifierValue(String identifierValue) {
        accessionNo = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
    
}
