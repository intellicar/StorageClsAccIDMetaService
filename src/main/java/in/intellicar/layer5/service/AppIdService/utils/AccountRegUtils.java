package in.intellicar.layer5.service.AppIdService.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.InstanceRegisterReq;
import in.intellicar.layer5.utils.JsonUtils;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class AccountRegUtils {

    public static Future<SHA256Item> checkAccountID(InstanceRegisterReq req, MySQLPool vertxMySQLClient, Logger logger) {
        String utf8name = new String(req.utf8bytes, StandardCharsets.UTF_8);
        String sql = "SELECT storagemetacls.instance_id from instance_register where utf8name = '" + utf8name + "'";
        Future<RowSet<Row>> future = vertxMySQLClient
                .query(sql)
                .execute();
        while (true) {
            synchronized (future) {

                try {
                    future.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (future.isComplete()) {
                RowSet<Row> rows = future.result();
                if (rows != null && rows.size() > 0) {
                    String hexID = rows.iterator().next().getString("instance_id");
                    SHA256Item instanceID = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
                    return Future.succeededFuture(instanceID);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
        }
    }

    public static SHA256Item generateAccountID(byte[] utf8bytes) {
        try {
            return SHA256Utils.getSHA256(utf8bytes);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Future<SHA256Item> getAccountID(InstanceRegisterReq req, MySQLPool vertxMySQLClient, Logger logger) {
        Future<SHA256Item> checkedAccountID = checkAccountID(req, vertxMySQLClient, logger);
        if (checkedAccountID.succeeded()) {
            return checkedAccountID;
        } else {
            String utf8name = new String(req.utf8bytes, StandardCharsets.UTF_8);
            ObjectNode metadata = JsonUtils.omap.createObjectNode();
            metadata.put("port", req.port);
            try {
                metadata.put("ip", InetAddress.getByAddress(req.ip).getCanonicalHostName());
            } catch (Exception ignored) {
            }

            SHA256Item instanceID = generateAccountID(req.utf8bytes);

            Future<RowSet<Row>> insertFuture = vertxMySQLClient.preparedQuery("INSERT INTO storagemetacls.instance_register (instance_id, utf8name, metadata) values (?, ?, ?)")
                    .execute(Tuple.of(instanceID.toHex(), utf8name, metadata.toString()));

            while (true) {
                synchronized (insertFuture) {

                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    return Future.succeededFuture(instanceID);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }
}