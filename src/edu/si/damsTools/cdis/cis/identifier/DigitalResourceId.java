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
public class DigitalResourceId implements IdentifierType {
    
    private String digitalResourceId;
    
    public String returnIdentifierCd() {
        return "dri";
    }
    
    public void setIdentifierValue(String identifierValue) {
        digitalResourceId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
}
