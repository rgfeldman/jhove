/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

/**
 *
 * @author rfeldman
 */
    
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.logging.Level;

import edu.si.CDIS.CIS.Database.Objects;

import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISCisMediaType;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.utilties.ErrorLog;
   
public class LinkToDamsAndCIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    HashMap <String, String> neverLinkedDamsIds;   
    
    /*  Method :       linkToCIS
        Arguments:      The CDIS object, and the StatisticsReport object
        Description:    link to CIS operation specific code starts here
        RFeldman 2/2015
    */
    public void link () {
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsIds = new HashMap <>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsIds ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in the CIS
        processNeverLinkedList ();    
        
    }
    
    private boolean linkObject (Integer cdisMapId, String cisIdentifier) {
        
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
        cdisObjectMap.setCisUniqueObjectId(Integer.toString(tmsObject.getObjectID()) );
        cdisObjectMap.createRecord();
        
        return true;
    }

    public boolean createNewLink(String cisIdentifier, String uoiId) {
        
        //Populate cdisMap Object based on renditionNumber
        CDISMap cdisMap = new CDISMap();
        cdisMap.setCisUniqueMediaId(cisIdentifier);     
        cdisMap.setDamsUoiid(uoiId);
        cdisMap.setCdisCisMediaTypeId(Integer.parseInt(CDIS.getProperty("linkedMediaTypeId")));
                
        Uois uois = new Uois();
        uois.setUoiid(uoiId);
        uois.populateName();        
        cdisMap.setFileName(uois.getName());
        
        boolean mapCreated = cdisMap.createRecord();
        if (!mapCreated) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
            return false;
        }
            
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("MIC");    
        boolean activityLogged = cdisActivity.insertActivity();
        if (!activityLogged) {
            logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
            return false;
        }
            
        boolean objectLinked = linkObject(cdisMap.getCdisMapId(), cisIdentifier);
        if (! objectLinked ) {
            logger.log(Level.FINER, "Error, unable to link objects to Media ");
            return false;
        }
        
        // ONLY refresh thumbnail IF the properties setting indicates we should.
        if (CDIS.getProperty("updateTMSThumbnail").equals("true") ) {
            
            //..and only refresh thumbnail if the media type is indicated to be in the CIS
            cdisMap.populateCdisCisMediaTypeId();   
            CDISCisMediaType cdisMediaType = new CDISCisMediaType();
            
            cdisMediaType.setCdisCisMediaTypeId(cdisMap.getCdisCisMediaTypeId());
            
            cdisMediaType.populateInCis();
            if (cdisMediaType.getInCisInd() == 'Y') {
            
                Thumbnail thumbnail = new Thumbnail();
                boolean thumbCreated = thumbnail.generate(cdisMap.getCdisMapId());
                            
                if (! thumbCreated) {
                    logger.log(Level.FINER, "CISThumbnailSync creation failed");
                    return false;
                }
            
                cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("CTS");    
            }
        }
        
        return true;
        
    }
    
    
    private void processNeverLinkedList() {
      
        String sql = null;    
        String currentIterationSql = null;
        String sqlTypeArr[] = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
              
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getCISIdentifier")) {   
                sql = key;    
            }
        }

        //Iterate though hash...the key is the select statement itself
        for (String uoiId : neverLinkedDamsIds.keySet()) {
            
            if (sql.contains("?FILE_NAME?") ) {
                currentIterationSql = sql.replace("?FILE_NAME?",neverLinkedDamsIds.get(uoiId));
            }
            else if (sql.contains("?OWNING_UNIT_UNIQUE_NAME?") ) {
                SiAssetMetaData siAsst = new SiAssetMetaData();
                siAsst.setUoiid(uoiId);
                siAsst.populateOwningUnitUniqueName();
                currentIterationSql = sql.replace("?OWNING_UNIT_UNIQUE_NAME?",siAsst.getOwningUnitUniqueName());
            }
            else {
                currentIterationSql = sql;
            }
            
            logger.log(Level.FINEST,"SQL " + currentIterationSql);
            
            try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(currentIterationSql);
                ResultSet rs = pStmt.executeQuery()   ) {   
                
                //Get the CIS identifier for the field selected from above
                if (rs.next()) {
                    String cisIdentifier = rs.getString(1);
                      
                    logger.log(Level.FINER, "Will create link for uoiid/CIS id: " + uoiId + " " + cisIdentifier);
                    //createNewLink(cisIdentifier, uoiId);
                    
                    try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                    
                }
                
                
            } catch (Exception e) {
                e.printStackTrace();
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
        
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("retrieveDamsIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery()   ) {
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                neverLinkedDamsIds.put(rs.getString("UOI_ID"),rs.getString(2));
                logger.log(Level.FINER,"Adding DAMS asset to lookup in CIS: {0}", rs.getString("UOI_ID") );
            }

        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        return;
        
    }
    
}
