/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.AAA;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.AAA.Database.TblDigitalMediaResource;
import edu.si.CDIS.CIS.AAA.Database.TblDigitalResource;
import edu.si.CDIS.CIS.AAA.Database.TblCollectionsOnlineImage;
import edu.si.CDIS.DAMS.Database.SiAssetMetadata;
import edu.si.CDIS.DAMS.Database.TeamsLinks;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.MediaTypeConfigR;
import edu.si.CDIS.utilties.ErrorLog;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CollectionRecord {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
        
    public boolean pointToUans(CDISMap cdisMap) {
         
        SiAssetMetadata siAsst = new SiAssetMetadata();
        //Get the uan 
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
                      
        boolean pathUpdated = false;
        
        switch (CDIS.getProjectCd()) {
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
                    CDISMap serviceCdisMap = new CDISMap();
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
                                
                    CDISMap serviceCdisMap = new CDISMap();
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
