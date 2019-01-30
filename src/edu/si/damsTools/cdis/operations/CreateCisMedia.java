/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.cdis.cis.tms.MediaRecord;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.cis.tms.modules.ModuleFactory;
import edu.si.damsTools.cdis.cis.tms.modules.ModuleType;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;

public class CreateCisMedia extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList <DamsRecord> damsRecordForCisCreate;
    private final ModuleType module;
        
    public CreateCisMedia() {
        damsRecordForCisCreate = new ArrayList();
        ModuleFactory moduleFactory = new ModuleFactory();
        module = moduleFactory.moduleChooser();  
    }
    
    
    /*  Method :        populateNeverLinkedImages
        Arguments:      
        Description:    populates the list of never linked dams Images  
        RFeldman 2/2015
    */
    
    private boolean populateDamsImagesList () {
           
        String sql = XmlUtils.returnFirstSqlForTag("DamsSelectList");          
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                DamsRecord damsRecord = new DamsRecord();
                damsRecord.setUoiId(rs.getString(1));
                damsRecord.setBasicData();
                damsRecordForCisCreate.add(damsRecord);
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error, unable to obtain list of UOI_IDs to integrate", e);
            return false;
        }
        return true;
    }
    
    /*  Method :        processDamsRecord
        Arguments:      
        Description:    prcoesses the DAMS record one at a time from the list  
        RFeldman 2/2015
    */
    private boolean processDamsRecord (DamsRecord damsRecord) { 
            
        //Create CDISMap entry for the record
        CdisMap cdisMap = new CdisMap();
        cdisMap.setDamsUoiid(damsRecord.getUois().getUoiid());
        cdisMap.setFileName(damsRecord.getUois().getName());
            
        //Find existing CDISMAP record, and store the cdis_map_id in object by using the uoiid
        //  If CDIS sent it to DAMS with the sendToHotfolder tool initially, there should be a record there already....so in that case
        //  We do not want an additional cdis_map record
        boolean mapRecordExists = cdisMap.populateIdFromUoiid();
           
        if (!mapRecordExists) {    
            //  In the cases where media was put into DAMS by something other than CDIS, there will be no existing map entry there
            //      and we will need to add it  
                
            boolean mapCreated = cdisMap.createRecord();
            if (!mapCreated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCDMP", "Error, Unable to create CDISMAP entry");  
                return false;
            }
        } 
            
        // populate modileId info, and get the recordID for the module
        if (! module.populateRecordId(damsRecord) ) {
            logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain Module ID Data");
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UNLCIS", "ERROR: Media Creation Failed"); 
        }
        
        MediaRecord mediaRecord = new MediaRecord();
        boolean mediaCreated = mediaRecord.create(damsRecord, module);
                            
        if ( ! mediaCreated ) {
            ErrorLog errorLog = new ErrorLog ();
            if (mediaRecord.getErrorCode() != null) {
                errorLog.capture(cdisMap, mediaRecord.getErrorCode() , "ERROR: Media Creation Failed"); 
            }
            else {
                errorLog.capture(cdisMap, "CRCISM", "ERROR: Media Creation Failed"); 
            }
            return false; //Go to the next record 
        }
                        
        logger.log(Level.FINER, "Media Creation complete, RenditionNumber for newly created media: " + mediaRecord.getMediaRenditions().getRenditionNumber());

        // Update the record with the cis_id 
        CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
        cdisCisIdentifierMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisCisIdentifierMap.setCisIdentifierCd("rnd");
        cdisCisIdentifierMap.setCisIdentifierValue(Integer.toString (mediaRecord.getMediaRenditions().getRenditionId()));
        cdisCisIdentifierMap.createRecord();
        
        // Create the thumbnail in the CIS
        Thumbnail thumbnail = new Thumbnail();
        boolean thumbCreated = thumbnail.generate(cdisMap.getCdisMapId());
                            
        if (! thumbCreated) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCIST", "ERROR: CIS Thumbnail Generation Failed"); 
            return false;
        }
            
        // Add activity record indicating Media Has been created
        CdisActivityLog activityLog = new CdisActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("CMC");
        activityLog.insertActivity();
          
        // Add activity record indicating Media Has been Linked
        activityLog = new CdisActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("LCC-" + XmlUtils.getConfigValue("lccIdType").toUpperCase());
        activityLog.insertActivity();
            
        // Add activity record indicating Media Has been Thumbnail Synced
        activityLog = new CdisActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
        activityLog.setCdisStatusCd("CTS");
        activityLog.insertActivity();
       
       return true;
    }
    
    
    /*  Method :        invoke
        Description:    The main driver for the ingest to CIS process 
    */
    public void invoke () {
         
        // Get a list of Renditions from DAMS that may need to be brought to the collection system (CIS)
        populateDamsImagesList ();
        
        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
        for (DamsRecord damsRecord  : this.damsRecordForCisCreate) {
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
            processDamsRecord (damsRecord);  
        
        }
        
        try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
         
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("assignCisRecordId");
        reqProps.add("cis");
        reqProps.add("cisConnString");
        reqProps.add("cisDriver");
        reqProps.add("cisInstance");
        reqProps.add("cisPass");
        reqProps.add("cisUser");
        reqProps.add("sqlFile");
        reqProps.add("damsRepo");
        reqProps.add("dupRenditionCheck");
        reqProps.add("idsPathId");
        reqProps.add("lccIdType");
        reqProps.add("mapDamsDataToCisRecordName");
        reqProps.add("mapFileNameToCisBarcode");
        reqProps.add("mapFileNameToCisRecordId");
        reqProps.add("mapFileNameToCisRecordName");
        reqProps.add("mediaStatusID");
        
        if (XmlUtils.getConfigValue("mapFileNameToCisRecordName") != null && XmlUtils.getConfigValue("mapFileNameToCisRecordName").equals("true") ) {
            reqProps.add("damsDelimiter");
            reqProps.add("damsToCisTrunc");
            reqProps.add("tmsDelimiter");
        }
        
        if (XmlUtils.getConfigValue("mapDamsDataToCisRecordName") != null &&  XmlUtils.getConfigValue("mapDamsDataToCisRecordName").equals("true") ) {
            reqProps.add("damsTablColToMap");
        }
        
        if (XmlUtils.getConfigValue("mapFileNameToCisBarcode") != null && XmlUtils.getConfigValue("mapFileNameToCisBarcode").equals("true") ) {
                    reqProps.add("appendTimeToNumber");
        }
        
        //add more required props here
        return reqProps;    
    }
     
    public boolean requireSqlCriteria () {
        return true;
    }
}
