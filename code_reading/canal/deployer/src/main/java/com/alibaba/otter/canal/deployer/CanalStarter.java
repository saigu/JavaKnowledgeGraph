package com.alibaba.otter.canal.deployer;

import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.server.CanalMQStarter;

/**
 * Canal server 启动类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.2
 */
public class CanalStarter {

    private static final Logger logger                    = LoggerFactory.getLogger(CanalStarter.class);

    private static final String CONNECTOR_SPI_DIR         = "/plugin";
    private static final String CONNECTOR_STANDBY_SPI_DIR = "/canal/plugin";

    private CanalController     controller                = null;
    private CanalMQProducer     canalMQProducer           = null;
    private Thread              shutdownThread            = null;
    private CanalMQStarter      canalMQStarter            = null;
    private volatile Properties properties;
    private volatile boolean    running                   = false;

    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties){
        this.properties = properties;
    }

    public boolean isRunning() {
        return running;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public CanalController getController() {
        return controller;
    }

    /**
     * 启动方法
     * note:
     * 1.启动CanalMQProducer
     * 2.启动CanalController
     * 3.注册shutdownHook
     * 4.启动canalMQStarter
     * 5.监听canalAdmin
     *
     * @throws Throwable
     */
    public synchronized void start() throws Throwable {
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
        //note 1.如果canal.serverMode不是tcp，SPI加载CanalMQProducer,并且启动CanalMQProducer
        //（回头可以深入研究下ExtensionLoader类的相关实现）
        if (!"tcp".equalsIgnoreCase(serverMode)) {
            ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
            canalMQProducer = loader
                    .getExtension(serverMode.toLowerCase(), CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR);
            if (canalMQProducer != null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(canalMQProducer.getClass().getClassLoader());
                canalMQProducer.init(properties);
                Thread.currentThread().setContextClassLoader(cl);
            }
        }
        //note 2.如果启动了canalMQProducer,就不使用canalWithNetty(这里的netty是用在哪里的？)
        if (canalMQProducer != null) {
            MQProperties mqProperties = canalMQProducer.getMqProperties();
            // disable netty
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            if (mqProperties.isFlatMessage()) {
                // 设置为raw避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
            }
        }

        logger.info("## start the canal server.");
        controller = new CanalController(properties);
        //note 3.启动canalController
        controller.start();
        logger.info("## the canal server is running now ......");
        //note 4.注册了一个shutdownHook,系统退出时执行相关逻辑
        shutdownThread = new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal server");
                    controller.stop();
                    //note 主线程退出
                    CanalLauncher.runningLatch.countDown();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal Server:", e);
                } finally {
                    logger.info("## canal server is down.");
                }
            }

        };
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        //note 5.启动canalMQStarter，
        if (canalMQProducer != null) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
            canalMQStarter.start(destinations);
            controller.setCanalMQStarter(canalMQStarter);
        }

        // start canalAdmin
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);

        //note 6.根据填写的canalAdmin的ip和port,启动canalAdmin,用netty做服务器
        //（netty原理、使用可以深入展开下）
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);

            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);

            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}",
                    port,
                    user,
                    passwd,
                    ip);

            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.parseInt(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }

        running = true;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }
}
