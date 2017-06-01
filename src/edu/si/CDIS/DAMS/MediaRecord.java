/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.MediaTypeConfigR;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.DAMS.Database.TeamsLinks;

/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public ArrayList<String> relatedUoiIds;
    private String uoiId;
    
    public String getUoiId() {
        return this.uoiId;
    }
    
    public void setUoiId(String uoiId) {
        this.uoiId = uoiId;
    }
    
    public boolean establishParentChildLink (CDISMap cdisMap) {
        
        //Get the mediaTypeId for the current record
        cdisMap.populateCdisCisMediaTypeId();
        
        //populate the Parent ID from the db
        MediaTypeConfigR mediaTypeConfigR = new MediaTypeConfigR();
        mediaTypeConfigR.setMediaTypeConfigId(cdisMap.getMediaTypeConfigId());
        
        //populate the parent and child ID from the db
        mediaTypeConfigR.populateChildAndParentOfId();
          
        CDISMap childCdisMap = new CDISMap();
        
        if (mediaTypeConfigR.getChildOfId() > 0 ) {
            
            CDISMap parentCdisMap = new CDISMap();
            
            boolean parentInfoPopulated = parentCdisMap.populateParentFileInfo(cdisMap.getCdisMapId() );
            if (parentInfoPopulated) {
                TeamsLinks teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                teamsLinks.setDestValue(parentCdisMap.getDamsUoiid());
                teamsLinks.setLinkType("CHILD");
                teamsLinks.createRecord();
        
                teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(parentCdisMap.getDamsUoiid());
                teamsLinks.setDestValue(cdisMap.getDamsUoiid());
                teamsLinks.setLinkType("PARENT");
                teamsLinks.createRecord();
            }
            else {
                logger.log(Level.FINER, "unable to obtain parent info ");
            }
        }
        
        if (mediaTypeConfigR.getParentOfId() > 0 ) {
            boolean childInfoPopulated = childCdisMap.populateChldFileInfo(cdisMap.getCdisMapId() );
            
            if (childInfoPopulated) {
                TeamsLinks teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(childCdisMap.getDamsUoiid());
                teamsLinks.setDestValue(cdisMap.getDamsUoiid());
                teamsLinks.setLinkType("CHILD");
                teamsLinks.createRecord();
        
                teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                teamsLinks.setDestValue(childCdisMap.getDamsUoiid());
                teamsLinks.setLinkType("PARENT");
                teamsLinks.createRecord();
            }
            else {
                logger.log(Level.FINER, "unable to obtain child info ");
            }
            
        }
        
        return true;
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
