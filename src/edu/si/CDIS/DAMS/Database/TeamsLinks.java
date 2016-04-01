/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TeamsLinks {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String destValue;
    private String linkType;
    private String srcValue;
    
    public String getDestValue() {
        return this.destValue == null ? "" : this.destValue;
    }
    
    public String getSrcValue() {
        return this.srcValue;
    }
    
    public String getLinkType() {
        return this.linkType;
    }
    
    public void setDestValue (String destValue) {
        this.destValue = destValue;
    }

    public void setLinkType (String linkType) {
        this.linkType = linkType;
    }
    
    public void setSrcValue (String srcValue) {
        this.srcValue = srcValue;
    }

    public boolean populateDestValue () {
        
        String sql = "SELECT dest_value FROM towner.teams_links " +
                    "WHERE src_value = '" + getSrcValue() + "' " +
                    "AND link_type = '" + getLinkType() + "' ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setDestValue (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain destValue from teams_links", e );
                return false;
        }
        return true;
    }
}
