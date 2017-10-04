/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.aaa;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.aaa.database.TblDigitalMediaResource;
import edu.si.damsTools.cdis.aaa.database.TblDigitalResource;
import edu.si.damsTools.cdis.aaa.database.TblCollectionsOnlineImage;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.dams.database.TeamsLinks;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.MediaTypeConfigR;
import edu.si.damsTools.cdisutilities.ErrorLog;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CollectionRecord {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
        
    public boolean pointToUans(CdisMap cdisMap) {
         
        SiAssetMetadata siAsst = new SiAssetMetadata();
        //Get the uan 
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
                      
        boolean pathUpdated = false;
        
        switch (DamsTools.getProjectCd()) {
            case "aaa_av" :
            
                MediaTypeConfigR cdiscisMediaTypeR = new MediaTypeConfigR();
                cdiscisMediaTypeR.setMediaTypeConfigId(cdisMap.getMediaTypeConfigId());
                cdiscisMediaTypeR.populateDescription();
                            
                TblDigitalMediaResource tblDigitalMediaResource = new TblDigitalMediaResource();
                            
                if (cdiscisMediaTypeR.getDescription().contains("Master")) {
                    //Get Child uoi_id (which is the service record)
                    TeamsLinks teamsLinks = new TeamsLinks();
                    teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                    teamsLinks.setLinkType("PARENT");
                                
                    //Get the serviceRecord for the master 
                    boolean serviceRetrieved = teamsLinks.populateDestValueNotDeleted();
                    if (! serviceRetrieved) {
                        return false;
                    }
                     
                    //Get the cis_unique_media_id for the serviceRecord
                    CdisMap serviceCdisMap = new CdisMap();
                    serviceCdisMap.setDamsUoiid(teamsLinks.getDestValue());
                    serviceCdisMap.populateCisUniqueMediaIdForUoiid();
                                    
                    //update the record in the CIS with the uan
                    tblDigitalMediaResource.setDigitalMediaResourceId(Integer.parseInt(serviceCdisMap.getCisUniqueMediaId() ));
                    tblDigitalMediaResource.setMasterFileUan(siAsst.getOwningUnitUniqueName());
                    pathUpdated = tblDigitalMediaResource.updateMasterFileUan();
                }
            
                else if (cdiscisMediaTypeR.getDescription().contains("Service")) {
                    tblDigitalMediaResource.setDigitalMediaResourceId(Integer.parseInt(cdisMap.getCisUniqueMediaId() ));
                    tblDigitalMediaResource.setServiceFileUan(siAsst.getOwningUnitUniqueName());         
                                
                    pathUpdated = tblDigitalMediaResource.updateServiceFileUan();                
                }
            
                else if (cdiscisMediaTypeR.getDescription().contains("Access")) {
                    //Get Child uoi_id (which is the service record)
                    TeamsLinks teamsLinks = new TeamsLinks();
                    teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                    teamsLinks.setLinkType("CHILD");
                                
                    boolean serviceRetrieved = teamsLinks.populateDestValueNotDeleted();
                    if (! serviceRetrieved) {
                        return false;
                    }
                                
                    CdisMap serviceCdisMap = new CdisMap();
                    serviceCdisMap.setDamsUoiid(teamsLinks.getDestValue());
                    serviceCdisMap.populateCisUniqueMediaIdForUoiid();
                                   
                    tblDigitalMediaResource.setDigitalMediaResourceId(Integer.parseInt(serviceCdisMap.getCisUniqueMediaId() ));                  
                    tblDigitalMediaResource.setAccessFileUan(siAsst.getOwningUnitUniqueName());
                                
                        pathUpdated = tblDigitalMediaResource.updateAccessFileUan();
                    }
                
            break;
            
            case "aaa_fndg_aid" :
                TblCollectionsOnlineImage tblCollectionsOnlineImage = new TblCollectionsOnlineImage();
                tblCollectionsOnlineImage.setDamsUan(siAsst.getOwningUnitUniqueName());
                tblCollectionsOnlineImage.setCollectionOnlineImageId(Integer.parseInt(cdisMap.getCisUniqueMediaId() ));
                
                pathUpdated = tblCollectionsOnlineImage.updateDamsUAN();
                
            break;
                
            case "aaa_images" :    
                 TblDigitalResource tblDigitalResource = new TblDigitalResource();
                //assign the uan and digital resourceID
                tblDigitalResource.setDamsUan(siAsst.getOwningUnitUniqueName());
                tblDigitalResource.setDigitalResourceId(Integer.parseInt(cdisMap.getCisUniqueMediaId() ));
                pathUpdated = tblDigitalResource.updateDamsUAN();
            break;
        }
        
        if (!pathUpdated) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPCISU", "Error: unable to update media Path");
                    
            return false;
        }
         
         return true;
         
     }
    
}
