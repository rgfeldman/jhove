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
public class CollectionsOnlineImageId implements IdentifierType {
    
    private String collectionsOnlineImageId;
    
    public String getIdentifierCd() {
        return "coi";
    }
    
    public String getIdentifierValue() {
        return collectionsOnlineImageId;
    }
   
    public void setIdentifierValue(String identifierValue) {
        collectionsOnlineImageId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
}
