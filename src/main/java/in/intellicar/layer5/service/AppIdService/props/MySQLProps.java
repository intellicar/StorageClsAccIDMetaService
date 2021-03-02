package in.intellicar.layer5.service.AppIdService.props;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class MySQLProps {
    public static String USERNAME_TAG = "username";
    public String username;
    public static String PASSWORD_TAG = "password";
    public String password;
    public static String JDBC_HOST_TAG = "jdbchost";
    public String jdbcHost;
    public static String DATABASE_TAG = "database";
    public String database;

    public boolean isValid;
    public MySQLProps() {
        isValid = false;
    }

    public static MySQLProps parseMySQLConfig(JsonNode configJson, Logger logger) {
        if (configJson == null || logger == null || !configJson.isObject())
            return null;

        try {
            if (!(configJson.has(USERNAME_TAG) && configJson.has(PASSWORD_TAG) && configJson.has(JDBC_HOST_TAG)))
                return null;
            MySQLProps mySQLProps = new MySQLProps();
            mySQLProps.username = configJson.get(USERNAME_TAG).asText();
            mySQLProps.password = configJson.get(PASSWORD_TAG).asText();
            mySQLProps.jdbcHost = configJson.get(JDBC_HOST_TAG).asText();
            mySQLProps.database = configJson.get(DATABASE_TAG).asText();
            mySQLProps.isValid = true;
            return mySQLProps;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while parsing netty config", e);
            return null;
        }
    }
}
