package edu.si.damsTools.cdis.cis.identifier;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author rfeldman
 */
public class OralHistoryTranscriptId implements IdentifierType {
    
    private String ohTranscriptId;
    
    public String getIdentifierCd() {
        return "oht";
    }
    
    public String getIdentifierValue() {
        return ohTranscriptId;
    }
    
    public void setIdentifierValue(String identifierValue) {
        ohTranscriptId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
}
