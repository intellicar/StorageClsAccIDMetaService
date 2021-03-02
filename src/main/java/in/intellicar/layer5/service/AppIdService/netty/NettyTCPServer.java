package in.intellicar.layer5.service.AppIdService.netty;

import in.intellicar.layer5.service.AppIdService.props.NettyProps;
import in.intellicar.layer5.utils.LogUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class NettyTCPServer extends Thread{
    public String serverID;
    public String scratchDir;
    public NettyProps nettyProps;
    public Logger logger;

    public ChannelHandler childHandler;
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workerGroup;
    public ChannelFuture serverBindFuture;

    public AtomicBoolean serverStartInProgress;
    public AtomicBoolean serverStopped;

    public NettyTCPServer(String serverID, String scratchDir, NettyProps nettyProps, ChannelHandler childHandler) {
        this.serverID = serverID;
        this.scratchDir = scratchDir;
        this.nettyProps = nettyProps;
        this.childHandler = childHandler;

        this.logger = LogUtils.createLogger(this.scratchDir, NettyTCPServer.class.getName() , "serverlog");
        this.serverStartInProgress = new AtomicBoolean(true);
        this.serverStopped = new AtomicBoolean(false);
    }

    public void run() {
        try {
            if (serverStopped.get()) {
                return;
            }


            bossGroup = new NioEventLoopGroup(nettyProps.bossthread);
            workerGroup = new NioEventLoopGroup(nettyProps.workerthread);


            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.childHandler(childHandler)
                    .option(ChannelOption.SO_BACKLOG, nettyProps.SO_BACKLOG)
                    .childOption(ChannelOption.SO_KEEPALIVE, nettyProps.SO_KEEPALIVE)
                    .childOption(ChannelOption.SO_LINGER, nettyProps.SO_LINGER)
                    .childOption(ChannelOption.TCP_NODELAY, nettyProps.TCP_NODELAY);

            serverBindFuture = serverBootstrap.bind(nettyProps.port).sync();
            logger.info("Netty listening on " + nettyProps.port);
            serverStartInProgress.set(false);
            if (serverBindFuture != null) {
                try {
                    logger.info("Waiting till the channel is closed");
                    serverBindFuture.channel().closeFuture().sync();
                    logger.info("Channel seems to be closed");
                    stopReceiver(1000);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception while stopping server", e);
                }
            }
            logger.info("Server thread exiting");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while creating server", e);
        }
    }

    public void stopReceiver(int timeout) {
        logger.info("Beacon receiver stop started");
        this.serverStopped.set(true);
        int timeoutcount = 10;
        while (serverStartInProgress.get() && timeoutcount > 0) {
            try {
                //this.producer.flush();
                Thread.sleep(timeout / timeoutcount);
                timeoutcount--;
            } catch (Exception e) {
            }
        }
        logger.info("Shutdown now");
        if (bossGroup != null) {
            try {
                bossGroup.shutdownGracefully();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while stopping bossgroup", e);
            }
        }
        if (workerGroup != null) {
            try {
                workerGroup.shutdownGracefully();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while stopping workerGroup", e);
            }
        }
        if (bossGroup != null) {
            try {
                bossGroup.terminationFuture().sync();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while stopping bossgroup terminationFuture", e);
            }
        }
        if (workerGroup != null) {
            try {
                workerGroup.terminationFuture().sync();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while stopping workerGroup terminationFuture", e);
            }
        }
        bossGroup = null;
        workerGroup = null;
        logger.info("Beacon receiver stopped");
    }
}
