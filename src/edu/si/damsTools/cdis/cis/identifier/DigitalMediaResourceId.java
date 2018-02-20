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
public class DigitalMediaResourceId implements IdentifierType {
    
    private String digitalMediaResourceId;
    
    public String returnIdentifierCd() {
        return "dmi";
    }
    
    public void setIdentifierValue(String identifierValue) {
        digitalMediaResourceId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
}
