/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.cis.tms.database.MediaRenditions;
import edu.si.damsTools.cdis.cis.tms.MediaRecord;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.database.CDISActivityLog;
import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdis.database.CDISObjectMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlSqlConfig;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;



public class CreateCisMedia extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList <String> uoiidsToLink;  
            
    public CreateCisMedia() {
 
    }
    
    
    /*  Method :        populateNeverLinkedImages
        Arguments:      
        Description:    populates the list of never linked dams Images  
        RFeldman 2/2015
    */
    
    private boolean populateNeverLinkedImages () {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        xml.setProjectCd(DamsTools.getProjectCd());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("DamsSelectList"); 
        
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
                    uoiidsToLink.add(rs.getString("UOI_ID"));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error, unable to obtain list of UOI_IDs to integrate", e);
                return false;
            }
        }
        
        return true;
        
    }
    
    /*  Method :        processUAN
        Arguments:      
        Description:    prcoesses the DAMS uans one at a time from the list  
        RFeldman 2/2015
    */
    private void processUoiid (String uoiId) { 

        Uois uois = new Uois();
        uois.setUoiid(uoiId);
        uois.populateName(); 
            
        //Create CDISMap entry for the record
        CDISMap cdisMap = new CDISMap();
        cdisMap.setDamsUoiid(uoiId);
        cdisMap.setFileName(uois.getName());
            
        //Find existing CDISMAP record, and store the cdis_map_id in object by using the uoiid
        //  If CDIS sent it to DAMS with the sendToHotfolder tool initially, there should be a record there already....so in that case
        //  We do not want an additional cdis_map record
        boolean mapRecordExists = cdisMap.populateIdFromUoiid();
           
        if (!mapRecordExists) {    
            //  In the cases where media was put into DAMS by something other than CDIS, there will be no existing map entry there
            //      and we will need to add it  
            cdisMap.setCdisCisMediaTypeId(Integer.parseInt(DamsTools.getProperty("mediaTypeConfigId")) );
                
            boolean mapCreated = cdisMap.createRecord();
            if (!mapCreated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCDMP", "Error, Unable to create CDISMAP entry");  
                return;
            }
        } 
        else {
            cdisMap.populateCdisCisMediaTypeId();
        }
            
        MediaRenditions mediaRendition = new MediaRenditions();
        MediaRecord mediaRecord = new MediaRecord();
            
        Integer objectId = mediaRecord.create(uois, mediaRendition);
                            
        if ( objectId == 0 ) {
            ErrorLog errorLog = new ErrorLog ();
            if (mediaRecord.getErrorCode() != null) {
                errorLog.capture(cdisMap, mediaRecord.getErrorCode() , "ERROR: Media Creation Failed"); 
            }
            else {
                errorLog.capture(cdisMap, "CRCISM", "ERROR: Media Creation Failed"); 
            }
            return; //Go to the next record 
        }
                        
        logger.log(Level.FINER, "Media Creation complete, RenditionNumber for newly created media: " + mediaRendition.getRenditionNumber() );

        // Update the CDISMAP record with the cis_id 
        cdisMap.setCisUniqueMediaId(Integer.toString (mediaRendition.getRenditionId()) );
        cdisMap.updateCisUniqueMediaId() ;
            
        // Create the thumbnail in the CIS
        Thumbnail thumbnail = new Thumbnail();
        boolean thumbCreated = thumbnail.generate(cdisMap.getCdisMapId());
                            
        if (! thumbCreated) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCIST", "ERROR: CIS Thumbnail Generation Failed"); 
            return;
        }
            
        CDISObjectMap cdisObjectMap = new CDISObjectMap ();
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.setCisUniqueObjectId(Integer.toString (objectId) );
        boolean objectMapCreated = cdisObjectMap.createRecord();
        if (! objectMapCreated) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCDOB", "ERROR: ObjectMap Creation Failed"); 
            return;
        }
            
        // Add activity record indicating Media Has been created
        CDISActivityLog activityLog = new CDISActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("CMC");
        activityLog.insertActivity();
          
        // Add activity record indicating Media Has been Linked
        activityLog = new CDISActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("LCC");
        activityLog.insertActivity();
            
        // Add activity record indicating Media Has been Thumbnail Synced
        activityLog = new CDISActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("CTS");
        activityLog.insertActivity();
    }
    
    
    /*  Method :        createMedia
        Arguments:      
        Description:    The main driver for the ingest to CIS process 
        RFeldman 2/2015
    */
    public void invoke () {
        
        this.uoiidsToLink = new ArrayList<>();
        
        // Get a list of Renditions from DAMS that may need to be brought to the collection system (CIS)
        populateNeverLinkedImages ();
        
        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
        for (String uoiId : uoiidsToLink) {
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
            processUoiid (uoiId);  
        
        }
        
        try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
         
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
     
}
