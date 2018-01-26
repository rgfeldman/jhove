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
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.xfer.FileSystemFile;
import java.io.File;


/**
 *
 * @author rfeldman
 */
public class Sftp implements DeliveryMethod{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
        
        SSHClient ssh = new SSHClient();
        
        try (SFTPClient sftp = ssh.newSFTPClient()) {
            
            String username = "dpoir";
            File privateKey = new File("~/.ssh/id_dpoir_ecdsa");
            KeyProvider keys = ssh.loadKeys(privateKey.getPath());
            ssh.authPublickey(username, keys);
            
            sftp.put(new FileSystemFile("/tmp/test.txt"), "/path/of/ftp/file");
 
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
