/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import edu.si.data.DataProvider;
import CDIS_redesigned.DAMSDatabase.SiAssetMetaData;
import CDIS_redesigned.xmlConfig;


/**
 *
 * @author rfeldman
 */
    
public class MetaData {
    
    /**
     * @param args the command line arguments
     */
    
    
    public void sync(Connection tmsConn, Connection damsConn, String RenditionID) {
        
        
        // read the XML config file and obtain the selectStatements
        xmlConfig xml = new xmlConfig();
        xml.read();
        
        // initialize renditionID lists for sync
        ArrayList<Integer> neverSyncedRenditionIdLst = new ArrayList<Integer>();
        ArrayList<Integer> sourceUpdatedRenditionIdLst = new ArrayList<Integer>();
        ArrayList<Integer> restrictUpdatedRenditionIdLst = new ArrayList<Integer>();
        
        // Grab all the records that have NEVER EVER been synced by CDIS yet
        neverSyncedRenditionIdLst = getNeverSyncedRendition (tmsConn);
        
        //Grab all the records that have been synced in the past, but have been updated
        sourceUpdatedRenditionIdLst = getSourceUpdatedRendition(tmsConn);
       
        // Grab all the records where the ids or isRestriced flag has been changed.  need to update IDS on these
        restrictUpdatedRenditionIdLst = getRestrictUpdatedRenditon(tmsConn);       
        
        // create the object to hold the metadata itself
        SiAssetMetaData siAsst = new SiAssetMetaData();
        
        // Loop through all the rendition IDs we determined we needed to sync
        // I have broken this up by different reasons for sync to make this more trackable and in case we want to exclude certain types of sync for certain units       
        if(!neverSyncedRenditionIdLst.isEmpty()) {
            processRenditionList(neverSyncedRenditionIdLst, xml, siAsst);
        }
        if(!neverSyncedRenditionIdLst.isEmpty()) {
            processRenditionList(sourceUpdatedRenditionIdLst, xml, siAsst);
        }
        
            // execute the SQL statment to obtain the metadata.  They key value is RenditionID
            //mapData(tmsConn, key, xml.getSelectStmt(), siAsst);
            
            //String uoiid = neverRend.getRenditionIDUoiid().get(key);
            
            //System.out.println("uoiid: " + uoiid);
            
            // generate the update Statement
            //generateUpdate(siAsst, uoiid);
        
            //update the SI_ASSET_METADATA table in database
            //siAsst.update;
            
            //IF update was successful, update the CDIS table with the latest timestamp
            
                                  
        
                
    }
    
    private void mapData(Connection tmsConn, Integer renditionID, ArrayList<String>metaDataSelect, SiAssetMetaData siAsst) {
        ResultSet rs;
        String sql;
        
        for(Iterator<String> iter = metaDataSelect.iterator(); iter.hasNext();) {
            
            // Get the sql value from the hasharray
            
            sql = iter.next();
            sql = sql.replace("?RenditionID?", renditionID.toString());
            
            System.out.println("select Statement: " + sql);
            
            rs = DataProvider.executeSelect(tmsConn, sql);
                       
            boolean successResult = true;
            
            // populate the metadata object with the values found from the database query
            try {
                while(rs.next()) {
                    if (sql.contains("AS credit")) {
                        if (rs.getString("credit") != null)  {
                            siAsst.setCredit(rs.getString("credit"));
                        }
                    }
                    if (sql.contains("AS group_title")) {
                        if (rs.getString("group_title") != null)  {
                            siAsst.setGroupTitle(rs.getString("group_title"));
                        }
                    }
                    if (sql.contains("AS is_restricted")) {
                        if (rs.getString("is_restricted") != null)  {
                            successResult = siAsst.setIsRestricted(rs.getString("is_restricted"));
                        }
                    }
                    if (sql.contains("AS keywords")) {
                        if (rs.getString("keywords") != null) {
                            siAsst.setKeywords(rs.getString("keywords"));
                        }
                    }
                    if (sql.contains("AS max_ids_size")) {
                        if (rs.getString("max_ids_size") != null) {
                            siAsst.setMaxIdsSize(rs.getString("max_ids_size"));
                        }
                    }
                    if (sql.contains("AS title")) {
                        if (rs.getString("title") != null) {
                            siAsst.setTitle(rs.getString("title"));
                        }
                    }
                    if (sql.contains("AS use_restrictions")) {
                        if (rs.getString("use_restrictions") != null) {
                           siAsst.setUseRestrictions(rs.getString("use_restrictions")); 
                        }
                    }
                    if (sql.contains("AS source_system_id")) {
                        if (rs.getString("source_system_id") != null) {
                            siAsst.setSourceSystemId(rs.getString("source_system_id"));
                        }
                    }
                    if (sql.contains("AS work_creation_date")) {
                        if (rs.getString("work_creation_date") != null) {
                            siAsst.setWorkCreationDate(rs.getString("work_creation_date"));
                        }
                        
                    }
                }
            } catch (SQLException e) {
              e.printStackTrace();
            }
            finally { 
                try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
            }
            
            if (! successResult) {
                System.out.println("validation error encountered in SET command: ");
            }
            
        }
        
    }
    
    // generate the update statement which will update the DAMS database
    private void generateUpdate (SiAssetMetaData siAsst, String uoiid) {
              
        String updateStatement = "UPDATE SI_ASSET_METADATA " +
                                " SET credit = '" + siAsst.getCredit() + "'," +
                                " group_title = '" + siAsst.getGroupTitle() + "'," +
                                " is_restricted = '" + siAsst.getIsRestricted() + "'," +
                                " keywords = '" + siAsst.getKeywords() + "'," +
                                " max_ids_size = '" + siAsst.getMaxIdsSize() + "'," +
                                " source_system_Id = '" + siAsst.getSourceSystemId() + "'," +
                                " title = '" + siAsst.getTitle() + "'," + 
                                " use_restrictions = '" + siAsst.getUseRestrictions() + "'," +                               
                                " work_creation_date = '" + siAsst.getWorkCreationDate() + "'" +
                                " public_use = 'Yes' " +
                                " WHERE uoi_id = '" + uoiid + "'";
        
        //we collected nulls in the set/get commands.  strip out the word null from the update statement
        updateStatement = updateStatement.replaceAll("null", "");
        
        //setUpdateSql(updateStatement);
                
        System.out.println("updateStatment: " + updateStatement);
        
    }
    
    private ArrayList<Integer> getNeverSyncedRendition (Connection tmsConn) {
    
        ArrayList<Integer> renditionIdList = new ArrayList<Integer>();
        
        String sql = "select top 5 RenditionID " +
                     "from CDIS " +
                     "where MetaDataSyncDate is NULL";
        
        ResultSet rs;
        
        rs = DataProvider.executeSelect(tmsConn, sql);
        
        try {
                while(rs.next()) {
                    renditionIdList.add(rs.getInt(1));
                }

        } catch (Exception e) {
            e.printStackTrace();
        }   
        finally { 
                try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        
        return renditionIdList;
    }
    
    private ArrayList<Integer> getSourceUpdatedRendition (Connection tmsConn) {
    
        ArrayList<Integer> renditionIdList = new ArrayList<Integer>();
        
        /*
        String sql = "need SQL HERE";
        
        ResultSet rs;
        
        rs = DataProvider.executeSelect(tmsConn, sql);
        
        try {
                while(rs.next()) {
                    renditionIdList.add(rs.getInt(1));
                }

        } catch (Exception e) {
            e.printStackTrace();
        }   
        finally { 
                try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        */
        
        return renditionIdList;
    }
    
    private ArrayList<Integer> getRestrictUpdatedRenditon (Connection tmsConn) {
    
        ArrayList<Integer> renditionIdList = new ArrayList<Integer>();
        
        /*
        String sql = "need SQL HERE";
        
        ResultSet rs;
        
        rs = DataProvider.executeSelect(tmsConn, sql);
        
        try {
                while(rs.next()) {
                    renditionIdList.add(rs.getInt(1));
                }

        } catch (Exception e) {
            e.printStackTrace();
        }   
        finally { 
                try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        */
        
        return renditionIdList;
    }

    void processRenditionList (ArrayList<Integer> RenditionIdList, xmlConfig xml, SiAssetMetaData siAsst) {
        
    }
}
