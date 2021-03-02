package in.intellicar.layer5.service.AppIdService.server;

import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.account.*;
import in.intellicar.layer5.beacon.storagemetacls.instance.*;
import in.intellicar.layer5.service.AppIdService.props.NettyProps;
import in.intellicar.layer5.utils.LogUtils;
import in.intellicar.layer5.utils.PathUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.vertx.core.Vertx;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class StClsAccIDMtChInit extends ChannelInitializer<SocketChannel> {
    public String scratchDir;
    public NettyProps nettyProps;
    public Vertx vertx;

    public Logger logger;
    public Layer5BeaconParser l5parser;

    public StClsAccIDMtChInit(String scratchDir, NettyProps nettyProps, Vertx vertx) {
        this.scratchDir = PathUtils.appendPath(scratchDir, "channelh");
        this.nettyProps = nettyProps;
        this.vertx = vertx;

        this.logger = LogUtils.createLogger(this.scratchDir, StClsAccIDMtChInit.class.getName(), "handler");
        l5parser = Layer5BeaconParser.getHandler(StClsAccIDMtChInit.class.getName(), this.logger);

        Layer5BeaconDeserializer storageMetaClsAPIDeser = new StorageClsMetaBeaconDeser();
        l5parser.registerDeserializer(storageMetaClsAPIDeser.getBeaconType(), storageMetaClsAPIDeser);
        RegisterVertxCodecsForAllPayloads();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().
                addLast(new IdleStateHandler((long)nettyProps.heartBeatInterval,0L,0L,
                        TimeUnit.MILLISECONDS)).
                addLast(new StClsAccIDMtConnHandler(this.scratchDir, this.logger, l5parser, vertx));
    }

    /**
     * Registers all the codecs for classes those extend StorageClsMetaPayload, to eventbus of vertx.
     * We tried using register() and registerDefaultCodec() on StorageClsMetaPayload class, but still vertx is
     * complaining about individual payload classes. So we are registering codec for every payload here.
     */
    private void RegisterVertxCodecsForAllPayloads()
    {
        for (PayloadTypes eachType : EnumSet.allOf(PayloadTypes.class))
        {
            switch(eachType)
            {
                case INSTANCE_REGISTER_REQ:
                    vertx.eventBus().registerDefaultCodec(InstanceRegisterReq.class,
                            new StClsAccIDMtPayloadCodec<>(InstanceRegisterReq.class, logger));
                    break;
                case INSTANCE_REGISTER_RSP:
                    vertx.eventBus().registerDefaultCodec(InstanceRegisterRsp.class,
                            new StClsAccIDMtPayloadCodec<>(InstanceRegisterRsp.class, logger));
                    break;
                case INSTANCE_BUCKET_REQ:
                    vertx.eventBus().registerDefaultCodec(InstanceIdToBuckReq.class,
                            new StClsAccIDMtPayloadCodec<>(InstanceIdToBuckReq.class, logger));
                    break;
                case INSTANCE_BUCKET_RSP:
                    vertx.eventBus().registerDefaultCodec(InstanceIdToBuckRsp.class,
                            new StClsAccIDMtPayloadCodec<>(InstanceIdToBuckRsp.class, logger));
                    break;
                case ACCOUNT_INSTANCE_REQ:
                    vertx.eventBus().registerDefaultCodec(AccountInstanceReq.class,
                            new StClsAccIDMtPayloadCodec<>(AccountInstanceReq.class, logger));
                    break;
                case ACCOUNT_INSTANCE_RSP:
                    vertx.eventBus().registerDefaultCodec(AccountInstanceRsp.class,
                            new StClsAccIDMtPayloadCodec<>(AccountInstanceRsp.class, logger));
                    break;
                case NAMESPACE_REGISTER_REQ:
                    vertx.eventBus().registerDefaultCodec(NamespaceRegReq.class,
                            new StClsAccIDMtPayloadCodec<>(NamespaceRegReq.class, logger));
                    break;
                case NAMESPACE_REGISTER_RSP:
                    vertx.eventBus().registerDefaultCodec(NamespaceRegRsp.class,
                            new StClsAccIDMtPayloadCodec<>(NamespaceRegRsp.class, logger));
                    break;
                case ACCOUNT_ID_REQ:
                    vertx.eventBus().registerDefaultCodec(AccountIDReq.class,
                            new StClsAccIDMtPayloadCodec<>(AccountIDReq.class, logger));
                    break;
                case ACCOUNT_ID_RSP:
                    vertx.eventBus().registerDefaultCodec(AccountIDRsp.class,
                            new StClsAccIDMtPayloadCodec<>(AccountIDRsp.class, logger));
                    break;
                case STORAGE_CLS_META_ERROR_RSP:
                    vertx.eventBus().registerDefaultCodec(StorageClsMetaErrorRsp.class,
                            new StClsAccIDMtPayloadCodec<>(StorageClsMetaErrorRsp.class, logger));
                    break;
                default:
                    logger.log(Level.SEVERE, "trying to register codec for payload " + eachType.getSubType() + " , missed registration ?");
            }
        }
    }
}
