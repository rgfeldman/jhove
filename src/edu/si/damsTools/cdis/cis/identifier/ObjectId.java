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
public class ObjectId implements IdentifierType {
    
    private String objectId;
    
    public String returnIdentifierCd() {
        return "obj";
    }
    
    public void setIdentifierValue(String identifierValue) {
        objectId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
        
}
