/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.wilderness;
import java.util.Properties;
import java.sql.Connection;
import java.util.Date;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

//import com.mendix.core.component.InternalCore;
//import com.mendix.logging.ILogNode;


//
// Here are the dbcp-specific classes.
// Note that they are only used in the setupDriver
// method. In normal use, your classes interact
// only with the standard JDBC API
//
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;


/**
 *
 * @author Allistair
 */
public class WildernessPoolingDriver {
    private WPDConfig config = new WPDConfig();
    private String connectURI = null;
    private final Properties properties = new Properties();
    private ExternalService EXTERNAL_SERVICE;
    private static ExternalServiceStatus EXTERNAL_SERVICE_STATUS = new ExternalServiceStatus();
    private Log log = LogFactory.getLog(WildernessPoolingDriver.class);
    private Logger logger = Logger.getLogger("");
    
    public WildernessPoolingDriver(String server, Integer port, String database, String user, String password, ExternalService externalService) throws Exception {
       this.connectURI = "jdbc:sqlserver://" + server ; 
       this.EXTERNAL_SERVICE = externalService;
       PoolingDriver(null, port, database, user, password) ;
    }
   public WildernessPoolingDriver(String server, Integer port, String database, String user, String password, ExternalService externalService, WPDConfig config) throws Exception {
       this.connectURI = "jdbc:sqlserver://" + server ; 
       this.EXTERNAL_SERVICE = externalService;
       this.config = config;
       PoolingDriver(null, port, database, user, password) ;
       
    }
	
    public WildernessPoolingDriver(String server, String instance , String database, String user, String password, ExternalService externalService) throws Exception {
        this.connectURI = "jdbc:sqlserver://" + server ;
        this.EXTERNAL_SERVICE = externalService;
        PoolingDriver(instance, -1, database, user, password) ;
        
    }

        public WildernessPoolingDriver(String server, String instance , String database, String user, String password, ExternalService externalService, WPDConfig config) throws Exception {
        this.connectURI = "jdbc:sqlserver://" + server ;
        this.EXTERNAL_SERVICE = externalService;
        this.config = config;
        PoolingDriver(instance, -1, database, user, password) ;
        
        
    }

    private void  PoolingDriver(String instance , Integer port, String database, String user, String password) throws Exception {
        //Setup connection string and properties
        logger.setLevel(Level.ALL);
        if (EXTERNAL_SERVICE_STATUS.getServiceStatus(EXTERNAL_SERVICE) == ProcessingStatus.STARTED){
            return;
        }
        if (EXTERNAL_SERVICE_STATUS.getServiceStatus(EXTERNAL_SERVICE) == ProcessingStatus.STARTING){
            Integer count = 0;
            while (EXTERNAL_SERVICE_STATUS.getServiceStatus(EXTERNAL_SERVICE) == ProcessingStatus.STARTING && count < 10){
                System.out.println("Waiting for start: " + count.toString() ); 
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for start: " + count.toString() );
                }
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
                count++;
            }
            return;
        }
        if (EXTERNAL_SERVICE_STATUS.getServiceStatus(EXTERNAL_SERVICE) == ProcessingStatus.STOPPED){
        
            if (!EXTERNAL_SERVICE_STATUS.setServiceStatus(EXTERNAL_SERVICE, ProcessingStatus.STARTING)){
                Integer count = 0;
                while (EXTERNAL_SERVICE_STATUS.getServiceStatus(EXTERNAL_SERVICE) == ProcessingStatus.STARTING && count < 10){
                    System.out.println("Waiting for start: " + count.toString() ); 
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for start: " + count.toString());
                    } 
                    try{
                        Thread.sleep(100);
                    }
                    catch(Exception e){
                    }
                    count++;
                }
                return;
            }
            
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("driverClassName", "com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource");
            properties.setProperty("database", database);
            properties.setProperty("password", password);

            if (port > 0){
                properties.setProperty( "port", port.toString());
            }
            if (instance != null){
                properties.setProperty( "instanceName",instance);
            }
                try{
                   InitialisePooling();
                }
                catch(Exception ex){
                    throw new Exception(ex);
                }
            EXTERNAL_SERVICE_STATUS.setServiceStatus(EXTERNAL_SERVICE,ProcessingStatus.STARTED);
        }

        
    }
	
    private void InitialisePooling() throws Exception{
        try{
            
            System.out.println("Starting Wilderness Pooling ...");
            log.info("Starting Wilderness Pooling for " + this.EXTERNAL_SERVICE.name());
             
            setupDriver();
            System.out.println("Wilderness Pooling successfully initailised");
            log.info("Wilderness Pooling successfully initailised for " + this.EXTERNAL_SERVICE.name());
        }
        catch (Exception ex){
            System.out.println("Wilderness Pooling initailised failed. " + ex.getMessage());
            log.error("Wilderness Pooling initailised failed. ", ex);
            throw ex;
        }
    } 
    private void setupDriver() throws Exception {
        
        String jdbcDriverName = "com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource";
        
        try {
            java.lang.Class.forName(jdbcDriverName).newInstance();
          }
         catch (  ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            System.out.println("Error when attempting to obtain DB Driver: " + jdbcDriverName + " on "+ new Date().toString() + e.getMessage());
            log.error("Error when attempting to obtain DB Driver: " + jdbcDriverName + " on "+ new Date().toString(),e);
            throw new Exception(e);
          }
        
        ConnectionFactory connectionFactory =
            new DriverManagerConnectionFactory(this.connectURI,this.properties);

        PoolableConnectionFactory poolableConnectionFactory =
            new PoolableConnectionFactory(connectionFactory, null);
        
        GenericObjectPoolConfig genConfig = new GenericObjectPoolConfig();
        genConfig.setMaxIdle(this.config.getMaxIdle());
        genConfig.setMaxTotal(this.config.getMaxActive());
        genConfig.setMinIdle(this.config.getMinIdle());
        genConfig.setMaxWaitMillis(this.config.getMaxWaitMillis());
        genConfig.setTimeBetweenEvictionRunsMillis(5000);
        genConfig.setTestWhileIdle(true);
                
        
        ObjectPool<PoolableConnection> connectionPool =
            new GenericObjectPool<>(poolableConnectionFactory,genConfig);
        
        
        
        
        
        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);
        

        //
        // Finally, we create the PoolingDriver itself...
        //
        Class.forName("org.apache.commons.dbcp2.PoolingDriver");
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        System.out.println("Driver : " + driver.toString());
        log.debug("Driver : " + driver.toString());

        

        //
        // ...and register our pool with it.
        //
        driver.registerPool(EXTERNAL_SERVICE.name(),connectionPool);
       

        //
        // Now we can just use the connect string "jdbc:apache:commons:dbcp:example"
        // to access our pool of Connections.
        //
    }

    public Connection GetPoolConnection() throws Exception{
        
        Connection conn = null;
        
        try {
            long startTime = System.currentTimeMillis();
            conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:" + this.EXTERNAL_SERVICE.name());
            PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
            ObjectPool<? extends Connection> connectionPool = driver.getConnectionPool(this.EXTERNAL_SERVICE.name());
            log.debug("NumActive: " + connectionPool.getNumActive() + "  NumIdle: " + connectionPool.getNumIdle());
            
            long endTime = System.currentTimeMillis();
            System.out.println("Total connection time: " + this.EXTERNAL_SERVICE.name() + " "  + (endTime - startTime) );
            log.debug("Open connection for " + this.EXTERNAL_SERVICE.name());
        } catch(SQLException e) {
            System.out.println("Create connection failed. " + e.getMessage());
            log.error("Create connection failed. ", e);
            throw new Exception(e);
        }
        return conn;
    }
    
    
    public void shutdownDriver() throws Exception {
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        boolean setServiceStatus = EXTERNAL_SERVICE_STATUS.setServiceStatus(EXTERNAL_SERVICE, ProcessingStatus.STOPPED);
        driver.closePool(this.EXTERNAL_SERVICE.name());
        log.info("Pooling driver shutdown for " + this.EXTERNAL_SERVICE.name());
    }
}
