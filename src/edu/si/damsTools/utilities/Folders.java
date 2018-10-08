/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class Folders {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public static boolean isEmpty (Path pathDir) {
        
        try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(pathDir)) {

            return !dirStream.iterator().hasNext();
        }
        catch (Exception e) {
            logger.log(Level.FINER, "Unable to check hotfolder for existance of files ");
            return false;
        }
    }  
    
    public static int returnCount (Path pathDir) {
       
        int numFiles = 0;
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pathDir)) {
            while(stream.iterator().hasNext() ) {
               numFiles = numFiles ++;
            }
        } catch (Exception e) {
           // I/O error encounted during the iteration, the cause is an IOException
           logger.log(Level.FINER, "Unable to count failed files");
       }
        
        return numFiles;
    } 
}
