package ddth.dzitdb.osgi;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.osgi.framework.BundleContext;

import ddth.dasp.framework.dbc.DbcpJdbcFactory;
import ddth.dasp.framework.osgi.BaseBundleActivator;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.ITableInfoBo;
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

        String schemaName = "vdn";
        String tableName = "dtbl_demo";
        dzitDao.deleteTable(schemaName, tableName);

        Map<IDzitDbDao.EColumnType, Boolean> columnInfo = new HashMap<IDzitDbDao.EColumnType, Boolean>();
        columnInfo.put(IDzitDbDao.EColumnType.INT, Boolean.TRUE);
        columnInfo.put(IDzitDbDao.EColumnType.DATETIME, Boolean.TRUE);
        dzitDao.createTable(schemaName, tableName, columnInfo);

        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);

        String id = "nbthanh";
        String key = "first_name";
        String subkey = "";
        Map<IDzitDbDao.EColumnType, Object> recordData = new HashMap<IDzitDbDao.EColumnType, Object>();
        recordData.put(IDzitDbDao.EColumnType.INT, 12345);
        recordData.put(IDzitDbDao.EColumnType.DATETIME, new Date());
        dzitDao.createRecord(schemaName, tableName, id, key, subkey, recordData);
    }
}
