/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.orahive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.Path;

public class Utilities {

	public static String readTextFile(String fileName)
		throws IOException {
		
		File file = new File(fileName);
		long fileSize = file.length();
		
		char[] text = new char[(int) fileSize];

		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		reader.read(text);
		reader.close();
		return new String(text);
	}
	
	public static String readLineFromStdIn() throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 
		return in.readLine(); 
	}
	
	public static boolean readYNFromStdIn() throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 
		String result = in.readLine(); 
		if(result.equalsIgnoreCase("y"))
			return true;
		if(result.equalsIgnoreCase("n"))
			return false;

		// Recurse...
		System.out.println("Please enter either y or n : ");
		return readYNFromStdIn();
	}
	
	public static String getOraHiveJarFile() {
		
		Path jarFilePath = new Path(Utilities.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		String result = jarFilePath.toUri().getPath();
		if(!result.endsWith(".jar"))
			result = result + Path.SEPARATOR_CHAR + Constants.ORAHIVE_JAR_FILENAME;
		return result;
	}

	public static String getOraHiveVersion() {
		
		final String IMPLEMENTATION_VERSION = "Implementation-Version";
		
		String jarFileName = getOraHiveJarFile();
		try {
			JarFile jar = new JarFile(jarFileName);
			Manifest mf = jar.getManifest();
			Attributes at = mf.getMainAttributes();
			String result = at.getValue(IMPLEMENTATION_VERSION);
			if(result == null)
				return "";
			
			return result;
		}
		catch(IOException ex) {
		}
		
		return "";
	}	
	
	public static String padLeft(String s, int n) {
		
	    return String.format("%1$#" + n + "s", s);  
	}

	public static String padRight(String s, int n) {
		
	     return String.format("%1$-" + n + "s", s);  
	}

	public static boolean oracleSessionHasBeenKilled(Exception exception) {
		
		Throwable ex = exception;
		
		while(ex != null) {
			if(ex instanceof SQLException &&
			((SQLException)ex).getErrorCode() == 28) // ORA-00028: your session has been killed
				return true;
				
			ex = ex.getCause();
		}
		
		return false;
	}	
}
