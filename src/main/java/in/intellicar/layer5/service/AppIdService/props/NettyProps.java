package in.intellicar.layer5.service.AppIdService.props;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class NettyProps {
    public static String BOSSTHREADS_TAG = "bossthread";
    public int bossthread = 5;

    public static String WORKTHREADS_TAG = "workerthread";
    public int workerthread = 20;

    public static String PORT_TAG = "port";
    public int port;

    public static String SO_BACKLOG_TAG = "SO_BACKLOG";
    public int SO_BACKLOG = 1024;

    public static String SO_KEEPALIVE_TAG = "SO_KEEPALIVE";
    public boolean SO_KEEPALIVE = true;

    public static String SO_LINGER_TAG = "SO_LINGER";
    public int SO_LINGER = 0;

    public static String TCP_NODELAY_TAG = "TCP_NODELAY";
    public boolean TCP_NODELAY = true;

    public boolean isValid;

    public static String HEARTBEAT_READ_TAG = "HEARTBEAT_READ";
    public long heartBeatInterval =  10 * 1000;

    public static String HEARTBEAT_CHECK_TAG = "HEARTBEAT_CHECK";
    public long heartBeatCheckInterval =  6* 60 * 1000;

    public static String CONN_CLOSE_TIMEOUT_TAG = "CONN_CLOSE_TIMEOUT";
    public long connectionCloseTiemout = 15 * 60 * 1000;

    public static String MAXBUFFER_SIZE_TAG = "MAXBUFFER_SIZE";
    public int maxBufferSize = 1024 * 16;

    public static String IS_CSA = "iscsa";
    public int isCsa = 0;

    public NettyProps() {
        isValid = false;
    }

    public static NettyProps parseJson(JsonNode configJson, Logger logger) {
        if (configJson == null || logger == null)
            return null;

        try {
            NettyProps nettyProps = new NettyProps();
            if (!configJson.has(PORT_TAG))
                return null;
            nettyProps.port = configJson.get(PORT_TAG).asInt();
            nettyProps.bossthread = configJson.has(BOSSTHREADS_TAG) ? configJson.get(BOSSTHREADS_TAG).asInt() : nettyProps.bossthread;
            nettyProps.workerthread = configJson.has(WORKTHREADS_TAG) ? configJson.get(WORKTHREADS_TAG).asInt() : nettyProps.workerthread;
            nettyProps.SO_BACKLOG = configJson.has(SO_BACKLOG_TAG) ? configJson.get(SO_BACKLOG_TAG).asInt() : nettyProps.SO_BACKLOG;
            nettyProps.SO_KEEPALIVE = configJson.has(SO_KEEPALIVE_TAG) ? configJson.get(SO_KEEPALIVE_TAG).asBoolean() : nettyProps.SO_KEEPALIVE;
            nettyProps.SO_LINGER = configJson.has(SO_LINGER_TAG) ? configJson.get(SO_LINGER_TAG).asInt() : nettyProps.SO_LINGER;
            nettyProps.TCP_NODELAY = configJson.has(TCP_NODELAY_TAG) ? configJson.get(TCP_NODELAY_TAG).asBoolean() : nettyProps.TCP_NODELAY;
            nettyProps.heartBeatInterval = configJson.has(HEARTBEAT_READ_TAG) ?
                    configJson.get(HEARTBEAT_READ_TAG).asLong() : nettyProps.heartBeatInterval;
            nettyProps.heartBeatCheckInterval = configJson.has(HEARTBEAT_CHECK_TAG) ?
                    configJson.get(HEARTBEAT_CHECK_TAG).asLong() : nettyProps.heartBeatCheckInterval;
            nettyProps.connectionCloseTiemout = configJson.has(CONN_CLOSE_TIMEOUT_TAG) ?
                    configJson.get(CONN_CLOSE_TIMEOUT_TAG).asLong() : nettyProps.connectionCloseTiemout;
            nettyProps.maxBufferSize = configJson.has(MAXBUFFER_SIZE_TAG) ?
                    configJson.get(MAXBUFFER_SIZE_TAG).asInt() : nettyProps.maxBufferSize;
            nettyProps.isCsa = configJson.has(IS_CSA)?configJson.get(IS_CSA).asInt():nettyProps.isCsa;
            nettyProps.isValid = true;
            return nettyProps;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while parsing netty config", e);
            return null;
        }
    }
}
