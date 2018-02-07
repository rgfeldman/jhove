/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.MediaTypeConfigR;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.database.TeamsLinks;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public ArrayList<String> relatedUoiIds;
    private String uoiId;
    
    public String getUoiId() {
        return this.uoiId;
    }
    
    public void setUoiId(String uoiId) {
        this.uoiId = uoiId;
    }
    
     /*  Method :        buildDamsRelationList
        Arguments:      
        Returns:        true for success, false for failures
        Description:    populates relatedUoiIdsToSync which will hold a list of related uoiIds (connected by parents or children relations in DAMS)
        RFeldman 12/2016
    */
    
    public void buildDamsRelationList () {
        // See if there are any related parent/children relationships in DAMS. We find the parents whether they were put into DAMS
        // by CDIS or not.  We get only the direct parent for now...later we may want to add more functionality
                
        //Build the DAMS child/parent relationships for the current uoiId
        relatedUoiIds = new ArrayList();
        
        TeamsLinks teamsLinks = new TeamsLinks();
        teamsLinks.setSrcValue(uoiId);
        teamsLinks.setLinkType("PARENT");
        boolean relatedRecRetrieved = teamsLinks.populateDestValueNotDeleted();
        
        if (relatedRecRetrieved ) {
            relatedUoiIds.add(teamsLinks.getDestValue());
        }
        
        teamsLinks.setLinkType("CHILD");
        relatedRecRetrieved = teamsLinks.populateDestValueNotDeleted();
  
        if (relatedRecRetrieved ) {
            relatedUoiIds.add(teamsLinks.getDestValue());
        }
        
    }

}
