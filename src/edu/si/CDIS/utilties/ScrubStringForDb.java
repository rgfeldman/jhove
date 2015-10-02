/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.utilties;

/**
 *
 * @author rfeldman
 */
public class ScrubStringForDb {
    
        public String scrubString(String inputString) {
          
        String newString;
        
        // remove & for easy insert into db
        newString = inputString.replaceAll("&", "and");
		
	//escape any single quotes
	newString = newString.replaceAll("'", "''");
        
        // remove leading and trailing spaces
        newString = newString.trim();
        
        return newString;
        
    }
        
}
