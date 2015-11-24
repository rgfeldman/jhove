/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import edu.si.CDIS.Database.CDISLinkToCIS;

/**
 *
 * @author rfeldman
 */
public class LinkToCISMdpp {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public void link (CDIS cdis) {
        //  The CDIS process will check CDIS for Ingest table for any entries with status RI. 
        
        CDISLinkToCIS cdisLinkToCis = new CDISLinkToCIS();
        
        cdisLinkToCis.getReadyToIntegrateRecords (cdis.damsConn);
        
        
        //  CDIS will compare and validate all the checksum values (vendor, post copy, DAMS, EMu) for each .tif image file.
        //  Update the CDIS map table to link the images (file-name, uoi_ID) with the EMu, IRN 
        //  Update status and date in CDIS activity log.

        
    }
    
}
