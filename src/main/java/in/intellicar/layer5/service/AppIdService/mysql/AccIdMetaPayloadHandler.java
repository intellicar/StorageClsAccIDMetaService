package in.intellicar.layer5.service.AppIdService.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.AccountIDReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.AccountIDRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.NsIdReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.accidservice.NsIdRsp;
import in.intellicar.layer5.beacon.storagemetacls.service.common.IPayloadRequestHandler;
import in.intellicar.layer5.service.AppIdService.utils.NameNodeUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;

import java.util.logging.Logger;

/**
 * @author krishna mohan
 * @version 1.0
 * @project StorageClsAccIDMetaService
 * @date 02/03/21 - 5:09 PM
 */
public class AccIdMetaPayloadHandler implements IPayloadRequestHandler {
    @Override
    public StorageClsMetaPayload getResponsePayload(StorageClsMetaPayload lRequestPayload, MySQLPool lVertxMySQLClient, Logger lLogger) {
        short subType = lRequestPayload.getSubType();
        PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

        switch (payloadType) {
            case ACCOUNT_ID_REQ:
                Future<SHA256Item> accountIDFuture = NameNodeUtils.getAccountID((AccountIDReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (accountIDFuture.succeeded()) {
                    return new AccountIDRsp((AccountIDReq) lRequestPayload, accountIDFuture.result());
                } else {
                    return new StorageClsMetaErrorRsp(accountIDFuture.cause().getLocalizedMessage(), lRequestPayload);
                }
            case NS_ID_REQ:
                Future<SHA256Item> nsIDFuture = NameNodeUtils.getNsId((NsIdReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (nsIDFuture.succeeded()) {
                    return new NsIdRsp((NsIdReq) lRequestPayload, nsIDFuture.result());
                } else {
                    return new StorageClsMetaErrorRsp(nsIDFuture.cause().getLocalizedMessage(), lRequestPayload);
                }

            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  lRequestPayload);
        }
    }
}
