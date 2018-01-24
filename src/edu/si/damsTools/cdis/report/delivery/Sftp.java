/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.report.DisplayFormat;
import edu.si.damsTools.cdis.report.rptFile.RptFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;


/**
 *
 * @author rfeldman
 */
public class Sftp implements DeliveryMethod{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
        
        SSHClient ssh = new SSHClient();
        
        try (SFTPClient sftp = ssh.newSFTPClient()) {
                
            ssh.loadKnownHosts();
            ssh.connect("host");
        
            ssh.authPassword("dpoir_user", "Cb6fdX0Uq1MmdYLI7dvUhA==");
            
            //sftp.put(new FileSystemFile("/path/of/local/file"), "/path/of/ftp/file");
 
        }
        catch (Exception e) {
            
        }
        finally {
            try {ssh.disconnect();} catch (Exception e) {logger.log(Level.SEVERE, "Unable to disconnect from sftp", e);
            }
        }
             
        return true;
    }
}
