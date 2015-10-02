/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.utilties;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 * @author rfeldman
 */
public class TruncateForDB {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private final Integer ADMIN_CONTENT_TYPE_LENGTH = 50;
    private final Integer ALTERNAME_IDENTIFIER_LENGTH = 40;
    private final Integer CAPTION_LENGTH = 4000;
    private final Integer CREDIT_LENGTH = 2000;
    private final Integer DIGITAL_ITEM_NOTES_LENGTH = 200;
    private final Integer DESCRIPTION_LENGTH = 4000;
    private final Integer GROUP_TITLE_LENGTH = 2000;
    private final Integer INTELLECTUAL_CONTENT_CREATOR_LENGTH = 1000;
    private final Integer KEYWORDS_LENGTH = 4000;
    private final Integer NAMED_PERSON_LENGTH = 2000;
    private final Integer NOTES_LENGTH = 4000;
    private final Integer OTHER_CONSTRAINTS_LENGTH = 2000;
    private final Integer PRIMARY_CREATOR_LENGTH = 1000;
    private final Integer RIGHTS_HOLDER_LENGTH = 2000;
    private final Integer SERIES_TITLE_LENGTH = 150;
    private final Integer TERMS_AND_RESTRICTIONS_LENGTH = 4000;
    private final Integer TITLE_LENGTH = 2000;
    private final Integer USE_RESTRICTIONS_LENGTH = 250;
    private final Integer WORK_CREATION_DATE_LENGTH = 255;
    
    public String truncateString(String column, String originalDataValue ) {
        
         
        
        //Set truncatedValue to be same as original value in case we dont have to truncate,
        //   we want the utility to return the same value fed in
        String truncatedValue = originalDataValue;
        
        switch (column) {
            
            case "admin_content_type" :
                if ( originalDataValue.length() > ADMIN_CONTENT_TYPE_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,ADMIN_CONTENT_TYPE_LENGTH -1);
                }
                break;
                
            case "alternate_identifier_1": 
                if ( originalDataValue.length() > ALTERNAME_IDENTIFIER_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,ALTERNAME_IDENTIFIER_LENGTH -1);
                }
                break;
            case "credit" :
                if ( originalDataValue.length() > CREDIT_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,CREDIT_LENGTH -1);
                }
                break;
                
            case "caption" :
                if ( originalDataValue.length() > CAPTION_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,CAPTION_LENGTH -1);
                }
                break;
                
            case "digital_item_notes" :
                if ( originalDataValue.length() > DIGITAL_ITEM_NOTES_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,DIGITAL_ITEM_NOTES_LENGTH -1);
                }
                break;
                           
            case "description" :
                if ( originalDataValue.length() > DESCRIPTION_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,DESCRIPTION_LENGTH-1);
                }
                break;
                         
            case "group_title" :
                if ( originalDataValue.length() > GROUP_TITLE_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,GROUP_TITLE_LENGTH-1);
                }
                         
            case "intellectual_content_creator" :
                if ( originalDataValue.length() > INTELLECTUAL_CONTENT_CREATOR_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,INTELLECTUAL_CONTENT_CREATOR_LENGTH-1);
                }
            
            case "keywords" :
                if ( originalDataValue.length() > KEYWORDS_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,KEYWORDS_LENGTH-1);
                }
                          
            case "named_person" :
                if ( originalDataValue.length() > NAMED_PERSON_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,NAMED_PERSON_LENGTH-1);
                }
                          
            case "notes" :
                   if ( originalDataValue.length() > NOTES_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,NOTES_LENGTH-1);
                }          
            case "other_constraints" :
                   if ( originalDataValue.length() > OTHER_CONSTRAINTS_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,OTHER_CONSTRAINTS_LENGTH-1);
                }          
            case "primary_creator" :
                   if ( originalDataValue.length() > PRIMARY_CREATOR_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,PRIMARY_CREATOR_LENGTH-1);
                }          
            case "rights_holder" :
                   if ( originalDataValue.length() > RIGHTS_HOLDER_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,RIGHTS_HOLDER_LENGTH-1);
                }          
            case "series_title" :
                   if ( originalDataValue.length() > SERIES_TITLE_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,SERIES_TITLE_LENGTH-1);
                }        
            case "terms_and_restrictions" :
                   if ( originalDataValue.length() > TERMS_AND_RESTRICTIONS_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,TERMS_AND_RESTRICTIONS_LENGTH-1);
                }          
            case "title" :
                   if ( originalDataValue.length() > TITLE_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,TITLE_LENGTH-1);
                }           
            case "use_restrictions" :
                   if ( originalDataValue.length() > USE_RESTRICTIONS_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,USE_RESTRICTIONS_LENGTH-1);
                }          
            case "work_creation_date" :
                   if ( originalDataValue.length() > WORK_CREATION_DATE_LENGTH ) {
                    truncatedValue = originalDataValue.substring(0,WORK_CREATION_DATE_LENGTH-1);
                }          
            default :                                 
                logger.log(Level.ALL, "Error attempting to map unhandled column: " + column);
                truncatedValue = "ERROR: UNSUPPORTED COLUMN";
            }
        
        return truncatedValue;
                
    }
        
    
}
