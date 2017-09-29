/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.database.CDISActivityLog;

import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.cdis.database.CDISMap;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;

import edu.si.damsTools.utilities.XmlReader;
import edu.si.damsTools.utilities.XmlData;

public class CISThumbnailSync extends Operation {

    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList <Integer> mapIdsToSync;  
    private ArrayList <XmlData> xmlObjList;
     
    /*  Method :        populateRenditionsToUpdate
        Arguments:      
        Description:    Populate a list of thumnails that need to be generated/updated 
        RFeldman 3/2015
    */
            
    public CISThumbnailSync() {

    }
    
    private boolean populateIdsToUpdate () {
        
        String sql = null;
        
        for(XmlData xmlInfo : xmlObjList) {
            xmlInfo.getCleanDataForAttribute("type","retrieveMapIds");
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                mapIdsToSync.add(rs.getInt("cdis_map_id"));
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain list of mapids to sync, returning", e);
            return false;
        }            
        return true;
        
    }
    
    /*  Method :        sync
        Arguments:      
        Description:    CISThumbnailSync sync driver 
        RFeldman 3/2015
    */
    public void invoke () {
 
        XmlReader xmlReader = new XmlReader();
        xmlObjList = new ArrayList();
        xmlObjList = xmlReader.parser(DamsTools.getOperationType(), "query");
        
        this.mapIdsToSync = new ArrayList <>();
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateIdsToUpdate ();
        
        //create the thumbnail in TMS from those DAMS images 
        for (Integer mapId : mapIdsToSync) {
            CDISMap cdisMap = new CDISMap();
            cdisMap.setCdisMapId(mapId);
            cdisMap.populateMapInfo();
            
            Thumbnail thumbnail = new Thumbnail();
            boolean blobUpdated = thumbnail.generate (mapId);
            
            if (! blobUpdated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCIST", "ERROR: CIS Thumbnail Generation Failed"); 
                continue;
            }
            
            // we have successfully updated the Blob if we got to this point, mark it as success in the db
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(mapId);
            cdisActivity.setCdisStatusCd("CTS");    
            cdisActivity.insertActivity();
                
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
        }
        
        //commit anything not committed yet
        try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("cis");
        reqProps.add("cisDriver");
        reqProps.add("cisConnString");
        reqProps.add("cisUser");
        reqProps.add("cisPass");
        
        //add more required props here
        return reqProps;    
    }
}
