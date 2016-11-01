/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CdisCisMediaTypeR;
import edu.si.CDIS.Database.TeamsLinks;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public boolean establishParentChildLink (CDISMap cdisMap) {
        
        //Get the mediaTypeId for the current record
        cdisMap.populateCdisCisMediaTypeId();
        
        //populate the Parent ID from the db
        CdisCisMediaTypeR cdisCisMediaTypeR = new CdisCisMediaTypeR();
        cdisCisMediaTypeR.setCdisCisMediaTypeId(cdisMap.getCdisCisMediaTypeId());
        
        //populate the parent and child ID from the db
        cdisCisMediaTypeR.populateChildAndParentOfId();
          
        CDISMap childCdisMap = new CDISMap();
        
        if (cdisCisMediaTypeR.getChildOfId() > 0 ) {
            
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
        
        if (cdisCisMediaTypeR.getParentOfId() > 0 ) {
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
}
