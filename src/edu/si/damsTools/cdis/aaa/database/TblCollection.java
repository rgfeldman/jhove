/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.aaa.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TblCollection {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
     
    private Integer collectionId;
    private String collcode;
    
    public String getCollcode () {
        return this.collcode;
    }
        
    public Integer getCollectionId () {
        return this.collectionId;
    }
        
    public void setCollcode (String collcode) {
        this.collcode = collcode;
    }
        
    public void setCollectionId (Integer collectionId) {
        this.collectionId = collectionId;
    }
    
    public boolean populateCollcode () {
        
        String sql =    "SELECT Collcode " +
                        "FROM  dbo.tblCollection " +
                        "WHERE collectionId = " + getCollectionId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setCollcode(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain collcode for CollectionID", e );
                return false;
        }
        return true;
    }
       
    
}
