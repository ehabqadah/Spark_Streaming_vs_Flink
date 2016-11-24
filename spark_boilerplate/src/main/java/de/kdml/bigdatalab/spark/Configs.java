package de.kdml.bigdatalab.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Configs 
 * @author ehab
 *
 */
public class Configs {

	private Configs(Properties props) {
		this.props = props;
	}

	private static Configs _instance = null;

	private Properties props;

	public static Configs getInstance() {

		if (_instance == null) {
			Properties props = new Properties();
			InputStream input = null;

			try {
				input = new FileInputStream("./target/config.properties");
				
				// load a properties file
				props.load(input);
				_instance = new Configs(props);
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		return _instance;
	}

	public String getStringProp(String propName) {

		return this.props.getProperty(propName);
	}

	public int getIntProp(String propName) {

		return Integer.parseInt(this.props.getProperty(propName));
	}

}
