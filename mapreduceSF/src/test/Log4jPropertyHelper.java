package test;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;



/**
 * Created by 我自己 on 2016/7/29.
 */
public class Log4jPropertyHelper {
    public static void updateLog4jConfiguration(Class<?> targetClass,
                                                String log4jPath) throws Exception {
        Properties customProperties = new Properties();
        FileInputStream fs = null;
        InputStream is = null;
        try {
            fs = new FileInputStream(log4jPath);
            is = targetClass.getResourceAsStream("/log4j.properties");
            customProperties.load(fs);
            Properties originalProperties = new Properties();
            originalProperties.load(is);
            for (Map.Entry<Object, Object> entry : customProperties.entrySet()) {
                originalProperties.setProperty(entry.getKey().toString(), entry
                        .getValue().toString());
            }
            LogManager.resetConfiguration();
            PropertyConfigurator.configure(originalProperties);
        }finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(fs);
        }
    }
}
