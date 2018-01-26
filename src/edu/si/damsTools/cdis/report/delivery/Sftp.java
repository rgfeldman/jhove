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
import net.schmizz.sshj.Config;
import net.schmizz.sshj.DefaultConfig;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 *
 * @author rfeldman
 */
public class Sftp implements DeliveryMethod{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
        
        /*SSHClient ssh = new SSHClient();
        Config config = new DefaultConfig();
        
        
        try (SFTPClient sftp = ssh.newSFTPClient()) {
            
            String username = "dpoir";
            File privateKey = new File("~/.ssh/id_dpoir_ecdsa");
            KeyProvider keys = ssh.loadKeys(privateKey.getPath());
            ssh.authPublickey(username, keys);
            
            logger.log(Level.FINEST, "sFtp'ing file ");    
            
            sftp.put(new FileSystemFile("/tmp/test.txt"), "/oss/developers/dpoir/test.txt");
 
        }
        catch (Exception e) {
            logger.log(Level.FINEST, "Error, unable to sftp file ", e); 
            return false;
        }
        finally {
            try {ssh.disconnect();} catch (Exception e) {logger.log(Level.SEVERE, "Unable to disconnect from sftp", e);
            }
        }*/
        
        try{
            //run the script that has the commands in it
            String returnVal = execCmd ("sftp_test");
            
            System.out.println("RETURNVAL: " + returnVal );
            
        }
        catch (Exception e) {
             System.out.println("ERROR");
        }
        
             
        return true;
    }
    
    public static String execCmd(String cmd) throws java.io.IOException {
        java.util.Scanner s = new java.util.Scanner(Runtime.getRuntime().exec(cmd).getInputStream()).useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
}
    
}
