
package edu.si.damsTools.cdisutilities;

import edu.si.damsTools.DamsTools;
import java.util.logging.Logger;


public class Transform {
    
     private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
     
     
    // start: 0, 1, 2, 3, ..., N 
    // stop: -M, ... , -3, -2, -1, 0 or N (1, 2, 3, ...)
    public String transform(String original, 
		String delimiter, 
		String newDelimiter, 
		int start, 
		int stop) {
        
        String[] splitted = original.split(delimiter);
	StringBuilder transformed = new StringBuilder();
	transformed.append(splitted[start]);
	int max = splitted.length - 1; 
	if (stop < 0) {
            max += stop;
	}
	else {
            if (stop != 0) {
                max = stop;
            }
            else {
                max = 0;
            }
        }
		
        for (int i=start+1; i<=max; i++) {				
            transformed.append(newDelimiter);
            transformed.append(splitted[i]);
        }

        return transformed.toString();
    }
	
	// Stop position not defined
    public String transform(String original, 
			String delimiter, 
			String newDelimiter, 
			int start) {
		
	return transform(original, delimiter,newDelimiter, start, 0);
    }
	
    // New delimiter not defined
    public String transform(String original, 
			String delimiter, 
			int start,
			int stop) {
		
	return transform(original, delimiter, delimiter, start, stop);
    }
	
    // New delimiter and stop position not defined
    public String transform(String original, 
		String delimiter, 
		int start) {
		return transform(original, delimiter, delimiter, start);
        }
    
}
