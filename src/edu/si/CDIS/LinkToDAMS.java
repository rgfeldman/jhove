/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

/**
 *
 * @author rfeldman
 */

import edu.si.CDIS.Database.CDISMap;
        
public class LinkToDAMS {
    
    public void link (CDIS cdis) {
        
        // now find all the unlinked images in DAMS (uoiid is null)
        CDISMap cdisMap = new CDISMap();
        
        
        // First look in all hot folder failed folders to check if there are files there, if there are, record them in error log
       
        // See if we can find the media in DAMS based on the filename and checksum combination
        
        // Move any .tif files from staging to NMNH EMu pick up directory (on same DAMS Isilon cluster)
        
        // Create an EMu_ready.txt file in the EMu pick up directory.   
        
        // Update CDIS map table to link file_name, DAMS checksum to UOI_ID.
       
        
    }
}
