package com.shpp.mentoring.okushin.task3;

import com.shpp.mentoring.okushin.exceptions.NotExistPropertyKeyException;
import com.shpp.mentoring.okushin.task4p2.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * class PropertyManager was created for handy reading property files, getting values from property file by keys
 * it has methods
 */
public class PropertyManager {
    private PropertyManager() {
    }

    private static final Logger logger = LoggerFactory.getLogger(PropertyManager.class);


    /**
     * Reads data from file.properties by FILE_NAME in UTF-8 format and writes this data to instance of Properties
     *
     * @param fileName - name of property file to be read
     * @param prop     - reference to Property object for storing read property file
     */
    public static void readPropertyFile(String fileName, Properties prop) {
        try (FileInputStream inputStream = new FileInputStream(fileName);
             InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            prop.load(isr);
            logger.debug("Property was successfully read");
        } catch (IOException e) {
            logger.error("Can't read property file{}", e.getMessage(), e);

        }
       /* try (InputStream inputStream = App.class.getClassLoader().getResourceAsStream(fileName)) {
            assert inputStream != null;
            try (InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                prop.load(isr);
                logger.debug("Property was successfully read");
            }
        } catch (IOException e) {
            logger.error("Can't read property file{}", e.getMessage(), e);
        }

        */
    }

    /**
     * returns string value which is tied with key from property file
     *
     * @param propKey - key for getting tied value from the property file
     * @param prop    - object Properties in which property file was read
     * @return string value which is tied with key from property file
     */
    public static String getStringPropertiesValue(String propKey, Properties prop) {

        if (prop.getProperty(propKey) == null) {
            throw new NotExistPropertyKeyException("Value of PROPERTY_KEY: " + propKey + " doesn't exist");
        }
        return prop.getProperty(propKey);
    }
    public static int  getIntPropertiesValue(String propKey, Properties prop) {
        if (prop.getProperty(propKey) == null) {
            throw new NotExistPropertyKeyException("Value of PROPERTY_KEY: " + propKey + " doesn't exist");
        }
        return Integer.parseInt(prop.getProperty(propKey));
    }


}
