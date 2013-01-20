package ddth.dzitdb.osgi;

import java.util.HashMap;
import java.util.Map;

import org.osgi.framework.BundleContext;

import ddth.dasp.framework.dbc.DbcpJdbcFactory;
import ddth.dasp.framework.osgi.BaseBundleActivator;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.jdbc.JdbcDzitDbDao;
import ddth.dzitdb.bo.jdbc.JdbcTableInfoDao;

public class BundleActivator extends BaseBundleActivator {

    private DbcpJdbcFactory jdbcFactory;

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        if (jdbcFactory != null) {
            jdbcFactory.destroy();
        }
        super.stop(bundleContext);
    }

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        super.start(bundleContext);

        jdbcFactory = new DbcpJdbcFactory();
        jdbcFactory.init();

        JdbcTableInfoDao tableInfoDao = new JdbcTableInfoDao();
        tableInfoDao.setBundleContext(bundleContext);
        tableInfoDao.setSqlPropsLocation("/ddth/dzitdb/bo/jdbc/dzitdb-manager.sql.xml");
        tableInfoDao.setJdbcFactory(jdbcFactory);
        tableInfoDao
                .setDbConnUrl("jdbc:mysql://localhost:3306/vdn?useUnicode=true&characterEncoding=utf-8");
        tableInfoDao.setDbDriver("com.mysql.jdbc.Driver");
        tableInfoDao.setDbUsername("vdn");
        tableInfoDao.setDbPassword("vdn");
        tableInfoDao.init();

        JdbcDzitDbDao dzitDao = new JdbcDzitDbDao();
        dzitDao.setBundleContext(bundleContext);
        dzitDao.setTableInfoDao(tableInfoDao);
        dzitDao.setSqlPropsLocation("/ddth/dzitdb/bo/jdbc/dzitdb-manager.sql.xml");
        dzitDao.setJdbcFactory(jdbcFactory);
        dzitDao.setDbConnUrl("jdbc:mysql://localhost:3306/vdn?useUnicode=true&characterEncoding=utf-8");
        dzitDao.setDbDriver("com.mysql.jdbc.Driver");
        dzitDao.setDbUsername("vdn");
        dzitDao.setDbPassword("vdn");
        dzitDao.init();

        Map<IDzitDbDao.EColumnType, Boolean> columnInfo = new HashMap<IDzitDbDao.EColumnType, Boolean>();
        columnInfo.put(IDzitDbDao.EColumnType.INT, Boolean.TRUE);
        columnInfo.put(IDzitDbDao.EColumnType.DATETIME, Boolean.TRUE);
        dzitDao.createTable("vdn", "demo", columnInfo);

        tableInfoDao.getTableInfo("vdn", "dtbl_demo");
    }
}
