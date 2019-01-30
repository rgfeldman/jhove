/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
        
public class Objects {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer objectID;
    private String objectNumber;
    
    public Integer getObjectId () {
        return this.objectID;
    }
    
    public String getObjectNumber () {
        return this.objectNumber;
    }
    
   
    public void setObjectId (Integer objectID) {
        this.objectID = objectID;
    }
    
    public void setObjectNumber (String objectNumber) {
        this.objectNumber = objectNumber;
    }
    
    public boolean populateIdForObjectNumber() {
        String sql = "SELECT ObjectId " +
                    "FROM objects " +
                    "WHERE objectNumber = '" + getObjectNumber() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setObjectId(rs.getInt(1));
                return true;
            }  
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain ObjectNumber for objectID", e );
        }
        return false;
    }
}

