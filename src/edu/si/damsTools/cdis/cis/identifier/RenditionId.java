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
public class RenditionId implements IdentifierType {
    
    private String renditionId;
    
    public String returnIdentifierCd() {
        return "ren";
    }
    
    public void setIdentifierValue(String identifierValue) {
        renditionId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
}
