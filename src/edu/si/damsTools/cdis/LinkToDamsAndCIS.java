/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

/**
 *
 * @author rfeldman
 */
    
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.logging.Level;

import edu.si.damsTools.cdis.aaa.database.TblDigitalMediaResource;
import edu.si.damsTools.cdis.aaa.database.TblDigitalResource;
import edu.si.damsTools.cdis.aaa.database.TblCollectionsOnlineImage;
import edu.si.damsTools.cdis.cis.iris.database.SI_IrisDAMSMetaCore;
import edu.si.damsTools.cdis.cis.tms.database.Objects;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.cis.tms.database.MediaRenditions;
import edu.si.damsTools.cdis.dams.MediaRecord;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.dams.database.SiPreservationMetadata;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdis.database.CDISObjectMap;
import edu.si.damsTools.cdis.database.CDISRefIdMap;
import edu.si.damsTools.cdis.database.CDISActivityLog;
import edu.si.damsTools.cdis.database.VFCUMediaFile;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlSqlConfig;
import java.sql.ResultSetMetaData;

import java.util.ArrayList;

import edu.si.damsTools.DamsTools;

   
public class LinkToDamsAndCIS extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    HashMap <String, String> neverLinkedDamsIds;   
    
    XmlSqlConfig xml;
            
    public LinkToDamsAndCIS() {
        xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        xml.setProjectCd(DamsTools.getProjectCd());
    }
    
    
    /*  Method :       linkToCIS
        Arguments:      The CDIS object, and the StatisticsReport object
        Description:    link to CIS operation specific code starts here
        RFeldman 2/2015
    */
    public void invoke () {
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsIds = new HashMap <>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsIds ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in the CIS
        processNeverLinkedList ();    
        
    }
    
    private boolean linkObjectAspace(Integer cdisMapId, String refId) {
        
        //Insert into CDISRefIdMap
        CDISRefIdMap cdisRefIdMap = new CDISRefIdMap();
        cdisRefIdMap.setCdisMapId(cdisMapId);
        cdisRefIdMap.setRefId(refId);
        cdisRefIdMap.createRecord();
        
        return true;
    }
    
    private boolean linkObjectAaaFndgAid (Integer cdisMapId, String cisIdentifier) {
        TblCollectionsOnlineImage tblCollectionOnlineImage = new TblCollectionsOnlineImage();
        
        tblCollectionOnlineImage.setCollectionOnlineImageId(Integer.parseInt(cisIdentifier));
        
        boolean collectionIdFound = tblCollectionOnlineImage.populateCollectionId();
        if (!collectionIdFound ) {
            logger.log(Level.FINER, "Error: unable to obtain object_id" );
            return false;
        } 
        
        //Insert into CDISObjectMap
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        cdisObjectMap.setCdisMapId(cdisMapId);
        cdisObjectMap.setCisUniqueObjectId(Integer.toString(tblCollectionOnlineImage.getCollectionId()) );
        cdisObjectMap.createRecord();
        
        return true;
    }
    
    
    private boolean linkObjectAaaAv (Integer cdisMapId, String cisIdentifier) {
        
        //get earliest objectId on the current renditionID 
        TblDigitalMediaResource tblDigitalMediaResource= new TblDigitalMediaResource();
        
        tblDigitalMediaResource.setDigitalMediaResourceId(Integer.parseInt(cisIdentifier));
        
        boolean collectionIdFound = tblDigitalMediaResource.populateCollectionId();
        if (!collectionIdFound ) {
            logger.log(Level.FINER, "Error: unable to obtain object_id" );
            return false;
        } 
        
        //Insert into CDISObjectMap
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        cdisObjectMap.setCdisMapId(cdisMapId);
        cdisObjectMap.setCisUniqueObjectId(Integer.toString(tblDigitalMediaResource.getCollectionId()) );
        cdisObjectMap.createRecord();
        
        return true;
    }
    
    
    private boolean linkObjectAaaImage (Integer cdisMapId, String cisIdentifier) {
        
        //get earliest objectId on the current renditionID 
        TblDigitalResource tblDigitalResource= new TblDigitalResource();
        
        tblDigitalResource.setDigitalResourceId(Integer.parseInt(cisIdentifier));
        
        boolean collectionIdFound = tblDigitalResource.populateCollectionId();
        if (!collectionIdFound ) {
            logger.log(Level.FINER, "Error: unable to obtain object_id" );
            return false;
        } 
        
        //Insert into CDISObjectMap
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        cdisObjectMap.setCdisMapId(cdisMapId);
        cdisObjectMap.setCisUniqueObjectId(Integer.toString(tblDigitalResource.getCollectionId()) );
        cdisObjectMap.createRecord();
        
        return true;
    }
    
    private boolean linkObjectIris (Integer cdisMapId, String cisIdentifier) {
        
        //get earliest objectId on the current renditionID 
        SI_IrisDAMSMetaCore irisObject= new SI_IrisDAMSMetaCore();
        irisObject.setImageLibId(cisIdentifier);
        
        boolean objectIdsFound = irisObject.populateItemAccnoFull();
        if (!objectIdsFound ) {
            logger.log(Level.FINER, "Error: unable to obtain object_id" );
            return false;
        } 
        
        //Insert into CDISObjectMap
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        cdisObjectMap.setCdisMapId(cdisMapId);
        cdisObjectMap.setCisUniqueObjectId(irisObject.getItemAccnoFull() );
        cdisObjectMap.createRecord();
        
        return true;
    }
        
    private boolean linkObjectTMS (Integer cdisMapId, String cisIdentifier) {
        
        //get earliest objectId on the current renditionID 
        Objects tmsObject= new Objects();
        
        boolean objectIdsFound = tmsObject.populateMinObjectIDByRenditionId(Integer.parseInt(cisIdentifier));
        if (!objectIdsFound ) {
            logger.log(Level.FINER, "Error: unable to obtain object_id" );
            return false;
        } 
        
        //Insert into CDISObjectMap
        CDISObjectMap cdisObjectMap = new CDISObjectMap();
        cdisObjectMap.setCdisMapId(cdisMapId);
        cdisObjectMap.setCisUniqueObjectId(Integer.toString(tmsObject.getObjectId()) );
        cdisObjectMap.createRecord();
        
        return true;
    }

    public boolean createNewLink(CDISMap cdisMap, String cisIdentifier, String cisIdentifierType, String uoiId) {
        
        //Populate cdisMap Object based on cis indicator if present
        if ( (cisIdentifierType).equals("CIS_UNIQUE_MEDIA_ID") ) {
            cdisMap.setCisUniqueMediaId(cisIdentifier);
        }
        
        cdisMap.setDamsUoiid(uoiId);

        Uois uois = new Uois();
        uois.setUoiid(uoiId);
        uois.populateName();        
        cdisMap.setFileName(uois.getName());
        
        cdisMap.populateMediaTypeId();
        
        //We have two conditions: Create a cdis_map entry where it does not exist...
        //OR link an cdis_map row (where we add BOTH rhe DAMS_UOIID and the CIS_UNIQUE identifier
        
        //Check if the map record exists already with null cisID and null dams_uoiid 
        boolean mapRecordExists = cdisMap.populateIdForNameNullUoiidandCisId();
        if (mapRecordExists) {
            cdisMap.updateCisUniqueMediaId();
            cdisMap.updateUoiid();
        }
        else {
            //if we need to create the map record, then create the new record
            boolean mapCreated = cdisMap.createRecord();
            if (!mapCreated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
                return false;
            }
               
        }
         
        boolean objectLinked = false;
        switch (DamsTools.getProperty("cis")) {
            case "aaa" :
                if(DamsTools.getProjectCd().equals("aaa_av")) {
                    objectLinked = linkObjectAaaAv(cdisMap.getCdisMapId(), cisIdentifier);
                }
                else if (DamsTools.getProjectCd().equals("aaa_fndg_aid") ) {
                     objectLinked = linkObjectAaaFndgAid(cdisMap.getCdisMapId(), cisIdentifier);
                }
                else {
                    objectLinked = linkObjectAaaImage(cdisMap.getCdisMapId(), cisIdentifier);
                }
                break;
            case "aSpace" :
                objectLinked = linkObjectAspace(cdisMap.getCdisMapId(), cisIdentifier);
                break;
            case "iris" :
                objectLinked = linkObjectIris(cdisMap.getCdisMapId(), cisIdentifier);
                break;
            case "tms" :
                objectLinked = linkObjectTMS(cdisMap.getCdisMapId(), cisIdentifier);
                //update the isColor/Dams flag
                MediaRenditions mediaRenditions = new MediaRenditions();
                mediaRenditions.setRenditionId(Integer.parseInt(cisIdentifier) );
                mediaRenditions.updateIsColor1();
        } 
        if (! objectLinked ) {
            logger.log(Level.FINER, "Error, unable to link objects to Media");
            return false;
        } 
        
        // populate the cdis vfcuId if it exists
        boolean vfcuIdPopulated = cdisMap.populateVfcuMediaFileId();
        if (vfcuIdPopulated) {
            
            //Get the checksum for the mediaFileId
            VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
            vfcuMediaFile.setVfcuMediaFileId(cdisMap.getVfcuMediaFileId());
            vfcuMediaFile.populateVendorChecksum();
            
            // We have a vfcumedia id, so we should have a checksum value. Update the DAMS checksum information in preservation module
            SiPreservationMetadata siPreservation = new SiPreservationMetadata();
            siPreservation.setUoiid(cdisMap.getDamsUoiid()); 
            siPreservation.setPreservationIdNumber(vfcuMediaFile.getVendorChecksum());
            if ( siPreservation.getPreservationIdNumber() != null  && ! siPreservation.getPreservationIdNumber().isEmpty() ) {
                boolean preservationInfoAdded = siPreservation.insertRow();
                if (! preservationInfoAdded) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPDAMP", "Error, unable to insert preservation data");
                    return false;
                }
            }
            
        }
        
        
        // ONLY refresh thumbnail IF the properties setting indicates we should.
        if ( ! (DamsTools.getProperty("updateTMSThumbnail") == null) && DamsTools.getProperty("updateTMSThumbnail").equals("true") ) {
           
            Thumbnail thumbnail = new Thumbnail();
            boolean thumbCreated = thumbnail.generate(cdisMap.getCdisMapId());
                            
            if (! thumbCreated) {
                logger.log(Level.FINER, "CISThumbnailSync creation failed");
                return false;
            }
            
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("CTS");
            cdisActivity.insertActivity();
        }
        return true;
    }
    
    private void processNeverLinkedList() {
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getCISIdentifier");
       
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < DamsTools.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }    
            
            for (String uoiId : neverLinkedDamsIds.keySet()) {
                 String sql = xml.getSqlQuery();
                 
                if (sql.contains("?FILE_NAME?") ) {
                    sql = sql.replace("?FILE_NAME?",neverLinkedDamsIds.get(uoiId));
                }
            
                if (sql.contains("?OWNING_UNIT_UNIQUE_NAME?") ) {
                    SiAssetMetadata siAsst = new SiAssetMetadata();
                    siAsst.setUoiid(uoiId);
                    siAsst.populateOwningUnitUniqueName();
                    sql = sql.replace("?OWNING_UNIT_UNIQUE_NAME?",siAsst.getOwningUnitUniqueName());
                }
                logger.log(Level.FINEST,"SQL " + sql);
            
                try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                    ResultSet rs = pStmt.executeQuery()   ) {   
                    
                    ResultSetMetaData rsmd = rs.getMetaData();
                    String cisIdentifierType = rsmd.getColumnName(1).toUpperCase();
                
                    //Get the CIS identifier for the field selected from above
                    if (rs.next()) {
                                
                        String cisIdentifier = rs.getString(1);
                        
                        logger.log(Level.FINER, "Will create link for uoiid/CIS id: " + uoiId + " " + cisIdentifier);
                        CDISMap cdisMap = new CDISMap();
                        boolean linkCreated = createNewLink(cdisMap, cisIdentifier, cisIdentifierType, uoiId);
                    
                    if (linkCreated) {
                    
                        //Link Parent/Children record
                        if ( (DamsTools.getProperty("linkHierarchyInDams") != null ) && (DamsTools.getProperty("linkHierarchyInDams").equals("true") ) ) {
                            MediaRecord mediaRecord = new MediaRecord();
                
                            mediaRecord.establishParentChildLink(cdisMap);
                        }
                        
                        CDISActivityLog cdisActivity = new CDISActivityLog();
                        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                        cdisActivity.setCdisStatusCd("LDC");
                        cdisActivity.insertActivity();
                        
                        cdisActivity = new CDISActivityLog();
                        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                        cdisActivity.setCdisStatusCd("LCC"); 
                        cdisActivity.insertActivity();
                        
                    }
                    
                    try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    try { if ( DamsTools.getCisConn()!= null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
                }
                
                
                } catch (Exception e) {
                    e.printStackTrace();
                }    
            }
            
        }
        
    }
    
    
    /*  Method :        populateNeverLinkedRenditions
        Arguments:      
        Description:    Populates a hash list that contains DAMS renditions that need to be linked 
                        with the Collection system (TMS)
        RFeldman 2/2015
    */
    private void populateNeverLinkedDamsIds () {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("retrieveDamsIds"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < DamsTools.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }    
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                     neverLinkedDamsIds.put(rs.getString("UOI_ID"),rs.getString(2));
                    logger.log(Level.FINER,"Adding DAMS asset to lookup in CIS: {0}", rs.getString("UOI_ID") );
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
            }
        }
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
    
}
