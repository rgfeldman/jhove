/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.tms.database.Objects;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class Tms implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String cisImageIdentifier;
    private String cisGroupIdentifier; 
    
    public void setUniqueImageIdentifier (String identifier) {
        this.cisImageIdentifier = identifier;
    }
    
    public String getCisImageIdentifier () {
        return this.cisImageIdentifier;
    }
    
    public String getGroupIdentifier () {
        return this.cisGroupIdentifier;
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        Objects objects = new Objects();
        objects.setObjectId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        objects.populateObjectNumberForObjectID();
                        
        return "Object: " + objects.getObjectNumber() ;
    }
    
    public boolean populateGroupIdForImageId() {
        
        //get earliest objectId on the current renditionID 
        String sql =    "select min(a.ObjectID) " +
                        "from Objects a, " +
                        "MediaXrefs b, " +
                        "MediaMaster c, " +
                        "MediaRenditions d " +
                        "where a.ObjectID = b.ID " +
                        "and b.MediaMasterID = c.MediaMasterID " +
                        "and b.TableID = 108 " +
                        "and c.MediaMasterID = d.MediaMasterID " +
                        "and d.RenditionID = " + cisImageIdentifier;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
   
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getString(1) + " For RenditionID: " + cisImageIdentifier);
                cisGroupIdentifier = rs.getString(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
        
    }
    
}
