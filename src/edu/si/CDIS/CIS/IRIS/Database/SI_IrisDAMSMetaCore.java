/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.IRIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class SI_IrisDAMSMetaCore {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String imageLibId;
    private String itemAccnoFull;
    

    public String getImageLibId () {
        return this.imageLibId;
    }
    
    public String getItemAccnoFull () {
        return this.itemAccnoFull;
    }
    
    
    public void setImageLibId (String imageLibId) {
        this.imageLibId = imageLibId;
    }
    
    public void setItemAccnoFull (String itemAccnoFull) {
        this.itemAccnoFull = itemAccnoFull;
    }
    
    public boolean populateItemAccnoFull () {
        
        String sql =    "SELECT itemAccnoFull " +
                        "FROM  SI_IrisDAMSMetaCore5 " +
                        "WHERE ImageLibId = '" + getImageLibId() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setItemAccnoFull(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain itemAccnoFull for ImageLibId", e );
                return false;
        }
        return true;
    }
}
