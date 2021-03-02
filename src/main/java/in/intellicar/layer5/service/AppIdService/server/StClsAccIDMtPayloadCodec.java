package in.intellicar.layer5.service.AppIdService.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class StClsAccIDMtPayloadCodec<T extends StorageClsMetaPayload> implements MessageCodec<T, T> {

    public Logger logger;
    private final Class<T> cls;

    public StClsAccIDMtPayloadCodec(Class<T> lClass, Logger logger) {
        this.logger = logger;
        cls = lClass;
    }

    @Override
    public void encodeToWire(Buffer buffer, T t) {
        byte[] bufferArr = new byte[t.getPayloadSize()];
        buffer.appendShort((short) bufferArr.length);
        buffer.appendByte((byte) t.getSubType());
        t.serialize(bufferArr, 0, bufferArr.length, logger);
        buffer.appendBytes(bufferArr);
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        short dataLength = buffer.getShort(pos);
        byte[] payload = buffer.getBytes(pos + 2, pos + 3 + dataLength);//3: 2 + 1(for subtype)
        return (T)new StorageClsMetaBeaconDeser().
                deserializePayload(payload, 0, payload.length, logger).data;
    }

    @Override
    public T transform(T t) {
        return t;
    }

    @Override
    public String name() {
        return cls.getSimpleName()+"Codec";
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
