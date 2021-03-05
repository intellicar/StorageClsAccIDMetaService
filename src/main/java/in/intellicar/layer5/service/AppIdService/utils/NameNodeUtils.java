package in.intellicar.layer5.service.AppIdService.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.AccountIDReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.NsIdReq;
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


public class NameNodeUtils {

    public static Future<SHA256Item> checkAccountID(AccountIDReq req, MySQLPool vertxMySQLClient, Logger logger) {
        String accountNameString = new String(req.accNameUtf8Bytes, StandardCharsets.UTF_8);
        String sql = "SELECT account_id from accounts.account_info where account_name = '" + accountNameString + "'";
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
                    String hexID = rows.iterator().next().getString("account_id");
                    SHA256Item accountIDSHA = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
                    return Future.succeededFuture(accountIDSHA);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
        }
    }

    public static SHA256Item generateAccountID(byte[] nameBytes, byte[] saltBytes) {

        byte[] saltyName = new byte[saltBytes.length + nameBytes.length];
        System.arraycopy(nameBytes, 0, saltyName, 0, nameBytes.length);
        System.arraycopy(saltBytes, 0, saltyName, nameBytes.length, saltBytes.length);

        try {
            return SHA256Utils.getSHA256(saltyName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Future<SHA256Item> getAccountID(AccountIDReq req, MySQLPool vertxMySQLClient, Logger logger) {
        Future<SHA256Item> checkedAccountID = checkAccountID(req, vertxMySQLClient, logger);
        if (checkedAccountID.succeeded()) {
            return checkedAccountID;
        } else {
            String salt = Long.toHexString(System.nanoTime());
            SHA256Item accountIDSHA = generateAccountID(req.accNameUtf8Bytes, salt.getBytes());
            String accountNameString = new String(req.accNameUtf8Bytes, StandardCharsets.UTF_8);
            Future<RowSet<Row>> insertFuture = vertxMySQLClient.preparedQuery("INSERT INTO accounts.account_info (account_name, salt, account_id) values (?, ?, ?)")
                    .execute(Tuple.of(accountNameString, salt, accountIDSHA.toHex()));

            while (true) {
                synchronized (insertFuture) {

                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    return Future.succeededFuture(accountIDSHA);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }

    public static Future<SHA256Item> checkNsID(NsIdReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        String namespaceString = new String(lReq.namespaceBytes, StandardCharsets.UTF_8);
        //TODO:: need to recheck this approach
        String sql = "SELECT namespace_id from accounts.namespace_info where " +
                "(namespace_name = '" + namespaceString + "' AND account_id = '" + lReq.accountID.toHex() + "')";
        Future<RowSet<Row>> future = lVertxMySQLClient
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
                    String hexID = rows.iterator().next().getString("namespace_id");
                    SHA256Item namespaceId = new SHA256Item(LittleEndianUtils.hexStringToByteArray(hexID));
                    return Future.succeededFuture(namespaceId);
                } else {
                    return Future.failedFuture(future.cause());
                }
            }
        }
    }

    public static SHA256Item generateNsId(SHA256Item lAccId, byte[] lNameBytes, byte[] saltBytes) {

        byte[] saltyName = new byte[SHA256Item.HASH_SIZE + 1 + lNameBytes.length + saltBytes.length];
        System.arraycopy(lAccId.hashdata, 0, saltyName, 0, SHA256Item.HASH_SIZE);
        saltyName[SHA256Item.HASH_SIZE] = '/';
        System.arraycopy(lNameBytes, 0, saltyName, SHA256Item.HASH_SIZE + 1, lNameBytes.length);
        System.arraycopy(saltBytes, 0, saltyName, SHA256Item.HASH_SIZE + 1 + lNameBytes.length, saltBytes.length);

        try {
            return SHA256Utils.getSHA256(saltyName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Future<SHA256Item> getNsId(NsIdReq lReq, MySQLPool lVertxMySQLClient, Logger lLogger) {
        Future<SHA256Item> checkedAccountID = checkNsID(lReq, lVertxMySQLClient, lLogger);
        if (checkedAccountID.succeeded()) {
            return checkedAccountID;
        } else {
            String salt = Long.toHexString(System.nanoTime());
            //TODO:: need to recheck this approach
            SHA256Item namespaceId = generateNsId(lReq.accountID, lReq.namespaceBytes, salt.getBytes());
            String namespaceString = new String(lReq.namespaceBytes, StandardCharsets.UTF_8);
            Future<RowSet<Row>> insertFuture = lVertxMySQLClient.preparedQuery("INSERT INTO accounts.namespace_info (namespace_name, account_id, salt, namespace_id) values (?, ?, ?, ?)")
                    .execute(Tuple.of(namespaceString, lReq.accountID.toHex(), salt, namespaceId.toHex()));

            while (true) {
                synchronized (insertFuture) {

                    try {
                        insertFuture.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (insertFuture.succeeded()) {
                    return Future.succeededFuture(namespaceId);
                } else {
                    return Future.failedFuture(insertFuture.cause());
                }
            }
        }
    }
}