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
import java.util.ArrayList;
import java.util.logging.Level;

import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
   
public class LinkDamsToCIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    ArrayList <String> neverLinkedDamsIds;   
    ArrayList <String> objectIds;
    
    /*  Method :       linkToCIS
        Arguments:      The CDIS object, and the StatisticsReport object
        Description:    link to CIS operation specific code starts here
        RFeldman 2/2015
    */
    public void link () {
        
        //Establish the hash to hold the unlinked DAMS rendition List
        this.neverLinkedDamsIds = new ArrayList <>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedDamsIds ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS
        processNeverLinkedList ();    
        
    }
    
    private void linkObject (Integer cdisMapId, String cisIdentifier) {
        
        //get ALL the objectIds on the current renditionID
       
        this.objectIds = new ArrayList<>();
        
        getObjectIdsForCisId(cisIdentifier);
        
        for (String objectId : objectIds) {
            //Insert into CDISObjectMap
            CDISObjectMap cdisObjectMap = new CDISObjectMap();
            cdisObjectMap.setCdisMapId(cdisMapId);
            cdisObjectMap.setCisUniqueObjectId(objectId);
            cdisObjectMap.createRecord();
        }
    }
    
    private boolean getObjectIdsForCisId (String cisIdentifier) {
        
        String sql = "SELECT id " +
                    "FROM mediaXrefs a, " +
                    "     mediaMaster b " +
                    "WHERE a.mediaMasterId = b.mediaMasterId " +
                    "AND   b.RenditionId = " + cisIdentifier +
                    " AND TableId = 108";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ){
 
            while (rs != null && rs.next()) {
                objectIds.add(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain object_ids for media", e );
                return false;
        }
        return true; 

    }
    
    public void createNewLink(String cisIdentifier, String uoiId) {
        
        //Populate cdisMap Object based on renditionNumber
        CDISMap cdisMap = new CDISMap();
        cdisMap.setCisUniqueMediaId(cisIdentifier);
        cdisMap.setDamsUoiid(uoiId);
                
        Uois uois = new Uois();
        uois.setUoiid(uoiId);
        uois.populateName();        
        cdisMap.setFileName(uois.getName());
           
        boolean mapCreated = cdisMap.createRecord();
            if (!mapCreated) {
            logger.log(Level.FINER, "Error, unable to create CDIS_MAP record ");
        }
            
        linkObject(cdisMap.getCdisMapId(), cisIdentifier);
        
        //Create the thumbnail image
        Thumbnail thumbnail = new Thumbnail();
        //boolean thumbCreated = thumbnail.generate(uoiId, Integer.parseInt(cisIdentifier));
                            
        //if (! thumbCreated) {
        //    logger.log(Level.FINER, "Thumbnail creation failed");
        //}
                               
        SiAssetMetaData siAsst = new SiAssetMetaData();
        // update the SourceSystemID in DAMS with the RenditionNumber
        boolean rowsUpdated = siAsst.updateDAMSSourceSystemID();

        if (! rowsUpdated) {
            logger.log(Level.FINER, "Error updating source_system_id in DAMS");
        }
        
    }
    
    
    private void processNeverLinkedList() {
      
        String sql = null;    
        String currentIterationSql = null;
        String sqlTypeArr[] = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
              
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("checkAgainstCIS")) {   
                sql = key;    
            }
        }

        //Iterate though hash...the key is the select statement itself
        for (String uoiId : neverLinkedDamsIds) {

            logger.log(Level.FINEST,"SQL " + currentIterationSql);
            
                              
            try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(currentIterationSql);
                ResultSet rs = pStmt.executeQuery()   ) {   
            
                String cisIdentifier = rs.getString(1);
                
                createNewLink(cisIdentifier, uoiId);
                
                
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
            
            if (sqlTypeArr[0].equals("retrieveDamsImages")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery()   ) {
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                neverLinkedDamsIds.add(rs.getString("UOI_ID"));
                logger.log(Level.FINER,"Adding DAMS asset to lookup in TMS: {0}", rs.getString("UOI_ID") );
            }

        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        return;
        
    }
    
}
