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
import edu.si.CDIS.CIS.Thumbnail;
   
public class LinkDamsAndCIS {
    
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
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS
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
        cdisMap.setCdisCisMediaTypeId(Integer.parseInt(CDIS.getProperty("cdisCisMediaTypeId")));
                
        Uois uois = new Uois();
        uois.setUoiid(uoiId);
        uois.populateName();        
        cdisMap.setFileName(uois.getName());
        
        boolean mapCreated = cdisMap.createRecord();
            if (!mapCreated) {
            logger.log(Level.FINER, "Error, unable to create CDIS_MAP record ");
            return false;
        }
            
        boolean objectLinked = linkObject(cdisMap.getCdisMapId(), cisIdentifier);
        if (! objectLinked ) {
            logger.log(Level.FINER, "Error, unable to link objects to Media ");
            return false;
        }
        
        // ONLY refresh thumbnail IF the properties setting indicates we should
        if (CDIS.getProperty("updateTMSThumbnail").equals("true") ) {
            Thumbnail thumbnail = new Thumbnail();
            boolean thumbCreated = thumbnail.generate(cdisMap.getCdisMapId());
                            
            if (! thumbCreated) {
                logger.log(Level.FINER, "CISThumbnailSync creation failed");
                return false;
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
            
            if (sql.contains("?BASE_NAME?") ) {
                currentIterationSql = sql.replace("?BASE_NAME?",neverLinkedDamsIds.get(uoiId));
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
                              
                    createNewLink(cisIdentifier, uoiId);
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
