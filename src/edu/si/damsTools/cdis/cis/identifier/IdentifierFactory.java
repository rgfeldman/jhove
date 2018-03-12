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
public class IdentifierFactory {
    
    public IdentifierFactory() {
        
    }
    
    public IdentifierType identifierChooser(String identifier) {
        
        IdentifierType identifierType = null;
        
        switch (identifier) {
            case "acc" :
                identifierType = null;
                break;
            case "coi" :
                identifierType = new CollectionsOnlineImageId();
                break;
            case "dmi" :
                identifierType = new DigitalMediaResourceId();
                break;
            case "dri" :
                identifierType = new DigitalResourceId();
                break;
            case "ead" :
                identifierType = new EadRefId();
                break;
            case "ohm" :
                identifierType = new OralHistoryMediaResourceId();
                break;
            case "oht" :
                identifierType = new OralHistoryTranscriptId();
                break;   
            case "obj" :
                identifierType = null;
                break;
            case "ren" :
                identifierType = null;
                break;
        }
        
        return identifierType;
    }
}
