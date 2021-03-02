package in.intellicar.layer5.service.AppIdService.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.service.AppIdService.props.MySQLProps;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */

public class MySQLQueryHandler extends Thread{
    Vertx vertx;
    String scratchDir;
    Logger logger;
    MySQLProps mySQLProps;
    EventBus eventBus;
    MySQLPool vertxMySQLClient;
    private LinkedBlockingDeque<Message<StorageClsMetaPayload>> eventQueue;

    public MySQLQueryHandler(Vertx vertx, String scratchDir, MySQLProps mySQLProps, Logger logger) {
        this.vertx = vertx;
        this.scratchDir = scratchDir;
        this.mySQLProps = mySQLProps;
        this.logger = logger;
        this.eventQueue = null;
    }

    public void init() {

        // Vrxt MySQL Client
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setHost(mySQLProps.jdbcHost)
                .setUser(mySQLProps.username)
                .setPassword(mySQLProps.password);
        // Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5);

        // Vertx Init
        vertxMySQLClient = MySQLPool.pool(connectOptions, poolOptions);

        vertxMySQLClient.getConnection().onComplete(ar -> {
            if (ar.succeeded()) {
                logger.info("Successfully connected to mysql database");
            } else {
                logger.log(Level.SEVERE, "Could not establish a mysql connection");
            }
        });

        eventQueue = new LinkedBlockingDeque<Message<StorageClsMetaPayload>>();
        eventBus = vertx.eventBus();
        eventBus.consumer("/mysqlqueryhandler", new Handler<Message<StorageClsMetaPayload>>() {
            @Override
            public void handle(Message<StorageClsMetaPayload> event) {
//                logger.info("Received msg:" + event.body().toJson(logger));
//                handleMySQLQuery(event);
                eventQueue.add(event);
            }
        });
    }

    public void run() {
        try {
            while (true) {
                Message<StorageClsMetaPayload> event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
//                Future<StorageClsMetaPayload> future = Future.future();

                if (event != null) {
                    event.reply(getResponsePayload(event.body(), vertxMySQLClient, logger));
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static StorageClsMetaPayload getResponsePayload(StorageClsMetaPayload requestPayload, MySQLPool vertxMySQLClient, Logger logger) {

        short subType = requestPayload.getSubType();
        PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

        switch (payloadType) {
            case ACCOUNT_INSTANCE_REQ:


            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  requestPayload);
        }
    }
}