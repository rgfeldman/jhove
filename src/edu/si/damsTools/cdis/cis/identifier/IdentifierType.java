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
public interface IdentifierType {
    
    public String returnIdentifierCd();
    
    public void setIdentifierValue(String identifierValue);
    
    public boolean overwriteExistingLinkId();
    
}
