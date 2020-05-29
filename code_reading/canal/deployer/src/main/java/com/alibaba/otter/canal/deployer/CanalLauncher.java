package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * canal独立版本启动的入口类
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String             CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger             logger               = LoggerFactory.getLogger(CanalLauncher.class);
    public static final CountDownLatch      runningLatch         = new CountDownLatch(1);
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("canal-server-scan"));

    /**
     * note:
     * 1.加载canal.properties的配置内容
     * 2.根据canal.admin.manager判断是否是admin控制,如果不是admin控制，就直接根据canal.properties的配置来了
     * 3.如果是admin控制，就新开一个单一线程池每隔五秒用http请求去admin上拉配置进行merge（这里依赖了instance模块的相关配置拉取的工具方法）
     * 4.核心是用canalStarter.start()启动（这里又开了多线程）
     * 5.使用CountDownLatch保持主线程存活
     * 6.收到关闭信号，CDL-1,然后关闭配置更新线程池，优雅退出
     * @param args
     */
    public static void main(String[] args) {
        try {
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            /**
             * note:
             * 可以手动指定配置路径名称
             */
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                /**
                 * note:
                 * 使用xxxx.getClass().getResource()获得的是代码所在类编译成class文件之后输出文件所在目录位置
                 * 而xxxx.getClass().getClassLoader().getResource()获得的是class loader所在路径
                 */
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            final CanalStarter canalStater = new CanalStarter(properties);
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
                String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
                String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
                boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties,
                        CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
                String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
                String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
                if (StringUtils.isEmpty(registerIp)) {
                    registerIp = AddressUtils.getHostIp();
                }
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
                        user,
                        passwd,
                        registerIp,
                        Integer.parseInt(adminPort),
                        autoRegister,
                        autoCluster);
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig == null) {
                    throw new IllegalArgumentException("managerAddress:" + managerAddress
                            + " can't not found config for [" + registerIp + ":" + adminPort
                            + "]");
                }
                Properties managerProperties = canalConfig.getProperties();
                // merge local
                managerProperties.putAll(properties);
                int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(managerProperties,
                        CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                        "5"));
                executor.scheduleWithFixedDelay(new Runnable() {

                    private PlainCanal lastCanalConfig;

                    public void run() {
                        try {
                            if (lastCanalConfig == null) {
                                lastCanalConfig = configClient.findServer(null);
                            } else {
                                PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                                if (newCanalConfig != null) {
                                    // 远程配置canal.properties修改重新加载整个应用
                                    canalStater.stop();
                                    Properties managerProperties = newCanalConfig.getProperties();
                                    // merge local
                                    managerProperties.putAll(properties);
                                    canalStater.setProperties(managerProperties);
                                    canalStater.start();

                                    lastCanalConfig = newCanalConfig;
                                }
                            }

                        } catch (Throwable e) {
                            logger.error("scan failed", e);
                        }
                    }

                }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
                canalStater.setProperties(managerProperties);
            } else {
                canalStater.setProperties(properties);
            }

            canalStater.start();
            runningLatch.await();
            executor.shutdownNow();
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
        }
    }

    /**
     * note:
     * 1.这里用Thread.setDefaultUncaughtExceptionHandler设置了一个全局的未捕获异常的处理.
     * 2.一般我们spring mvc的应用，会通过设置DefaultHandlerExceptionResolver来做全局处理
     *
     */
    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("UnCaughtException", e);
            }
        });
    }

}
