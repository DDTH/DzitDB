package ddth.dzitdb.bo.jdbc;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddth.dasp.common.id.IdGenerator;
import ddth.dasp.common.logging.JdbcLogEntry;
import ddth.dasp.common.logging.JdbcLogger;
import ddth.dasp.framework.bo.jdbc.BaseJdbcBoManager;
import ddth.dasp.framework.bo.jdbc.IJdbcBoManager;
import ddth.dasp.framework.dbc.JdbcUtils;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.ITableInfoBo;
import ddth.dzitdb.bo.ITableInfoDao;
import ddth.dzitdb.bo.ITableInfoVo;

public class JdbcDzitDbDao extends BaseJdbcBoManager implements IDzitDbDao {

    private final Logger LOGGER = LoggerFactory.getLogger(JdbcDzitDbDao.class);

    protected final static String FIELD_SCHEMA_NAME = "schemaName";
    protected final static String FIELD_TABLE_NAME = "tableName";
    protected final static String FIELD_COLUMN_TYPE = "columnType";
    protected final static String FIELD_HAS_INDEX = "hasIndex";

    private ITableInfoDao tableInfoDao;

    public ITableInfoDao getTableInfoDao() {
        return tableInfoDao;
    }

    public void setTableInfoDao(ITableInfoDao tableInfoDao) {
        this.tableInfoDao = tableInfoDao;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tableExists(String schemaName, String tableName) {
        return tableInfoDao.getTableInfo(schemaName, tableName) != null;
    }

    /*--------------------------------------------------------------------------------*/

    protected String calcCacheKey(String schemaName, String tableName, String id) {
        String cacheKey = schemaName + "." + tableName + "." + id;
        return cacheKey;
    }

    protected String calcCacheKey(String schemaName, String tableName, String id, String key) {
        String cacheKey = schemaName + "." + tableName + "." + id + "." + key;
        return cacheKey;
    }

    protected String calcCacheKey(String schemaName, String tableName, String id, String key,
            String subkey) {
        String cacheKey = schemaName + "." + tableName + "." + id + "." + key + "." + subkey;
        return cacheKey;
    }

    /*--------------------------------------------------------------------------------*/

    protected String normalizeTableName(String tableName) {
        if (!tableName.startsWith(IDzitDbDao.TABLE_PREFIX)) {
            tableName = IDzitDbDao.TABLE_PREFIX + tableName;
        }
        return tableName;
    }

    protected ITableInfoBo loadTableInfo(String schemaName, String tableName) {
        tableName = normalizeTableName(tableName);
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        return tableInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(String schemaName, String tableName,
            Map<IDzitDbDao.EColumnType, Boolean> columnInfo) {
        tableName = normalizeTableName(tableName);

        ITableInfoVo tableInfo = tableInfoDao.createTableInfoVoObj().setSchemaName(schemaName)
                .setTableName(tableName)
                .setTableInfoEncodedString(ITableInfoBo.DEFAULT_JSON_TABLE_INFO_ENCODED_STRING);
        for (Entry<IDzitDbDao.EColumnType, Boolean> entry : columnInfo.entrySet()) {
            Boolean hasIndex = entry.getValue();
            tableInfo.addColumn(entry.getKey(), (hasIndex != null && hasIndex) ? true : false);
        }

        try {
            if (tableInfoDao instanceof IJdbcBoManager) {
                ((IJdbcBoManager) tableInfoDao).startTransaction();
            }
            startTransaction();

            // first: create table schema
            {
                String[] sqlKey = new String[] { "sql.createTable" };
                Map<String, Object> params = new HashMap<String, Object>();
                params.put(FIELD_SCHEMA_NAME, schemaName);
                params.put(FIELD_TABLE_NAME, tableName);
                executeStoredProcedure(sqlKey, params);
            }

            // second: add columns
            {
                String[] sqlKey = new String[] { "sql.addField" };
                Map<String, Object> params = new HashMap<String, Object>();
                params.put(FIELD_SCHEMA_NAME, schemaName);
                params.put(FIELD_TABLE_NAME, tableName);
                for (Entry<IDzitDbDao.EColumnType, Boolean> entry : columnInfo.entrySet()) {
                    Boolean hasIndex = entry.getValue();
                    params.put(FIELD_COLUMN_TYPE, JdbcTableInfoBo.columnTypeToStr(entry.getKey()));
                    params.put(FIELD_HAS_INDEX, (hasIndex != null && hasIndex) ? 1 : 0);
                    executeStoredProcedure(sqlKey, params);
                }
            }

            // third: create table info
            {
                tableInfoDao.createTableInfo(tableInfo);
            }
        } catch (Exception e) {
            try {
                if (tableInfoDao instanceof IJdbcBoManager) {
                    ((IJdbcBoManager) tableInfoDao).cancelTransaction();
                }
            } catch (Exception ex) {
                LOGGER.warn(ex.getMessage(), ex);
            }

            try {
                cancelTransaction();
            } catch (Exception ex) {
                LOGGER.warn(ex.getMessage(), ex);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (tableInfoDao instanceof IJdbcBoManager) {
                    ((IJdbcBoManager) tableInfoDao).finishTransaction();
                }
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }

            try {
                finishTransaction();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteTable(String schemaName, String tableName) {
        tableName = normalizeTableName(tableName);

        // check if table exists
        ITableInfoBo tableInfo = loadTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return;
        }

        try {
            if (tableInfoDao instanceof IJdbcBoManager) {
                ((IJdbcBoManager) tableInfoDao).startTransaction();
            }
            startTransaction();

            // first: remove table info
            tableInfoDao.deleteTableInfo(tableInfo);

            // second: drop table
            String[] sqlKey = new String[] { "sql.dropTable" };
            Map<String, Object> params = new HashMap<String, Object>();
            params.put(FIELD_SCHEMA_NAME, schemaName);
            params.put(FIELD_TABLE_NAME, schemaName);
            executeStoredProcedure(sqlKey, params);
        } catch (Exception e) {
            try {
                if (tableInfoDao instanceof IJdbcBoManager) {
                    ((IJdbcBoManager) tableInfoDao).cancelTransaction();
                }
            } catch (Exception ex) {
            }

            try {
                cancelTransaction();
            } catch (Exception ex) {
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (tableInfoDao instanceof IJdbcBoManager) {
                    ((IJdbcBoManager) tableInfoDao).finishTransaction();
                }
            } catch (Exception e) {
            }

            try {
                finishTransaction();
            } catch (Exception e) {
            }
        }
    }

    protected Map<String, Object> populateParams(String id, String key, String subkey) {
        return populateParams(id, key, subkey, null);
    }

    protected Map<String, Object> populateParams(String id, String key, String subkey,
            Map<IDzitDbDao.EColumnType, Object> recordData) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(JdbcTableInfoBo.COL_ID, id);
        params.put(JdbcTableInfoBo.COL_KEY, key);
        params.put(JdbcTableInfoBo.COL_SUBKEY, subkey);
        // params.put(JdbcTableInfoBo.COL_TYPE, 0);
        if (recordData != null) {
            for (Entry<IDzitDbDao.EColumnType, Object> entry : recordData.entrySet()) {
                String colName = JdbcTableInfoBo.columnTypeToColumnName(entry.getKey());
                Object colValue = entry.getValue();
                params.put(colName, colValue);
            }
        }
        return params;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createRecord(String schemaName, String tableName, String id, String key,
            String subkey, Map<IDzitDbDao.EColumnType, Object> recordData) {
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return null;
        }
        String cacheKeySubkey = null, cacheKeyKey = null;
        if (StringUtils.isBlank(id)) {
            id = IdGenerator.getInstance(IdGenerator.getMacAddr()).generateId64Ascii();
        } else {
            cacheKeyKey = calcCacheKey(schemaName, tableName, id, key);
            cacheKeySubkey = calcCacheKey(schemaName, tableName, id, key, subkey);
        }
        String sql = tableInfo.getSqlInsert();
        try {
            long startTimestamp = System.currentTimeMillis();
            Map<String, Object> params = populateParams(id, key, subkey, recordData);
            Connection conn = getConnection();
            if (conn == null) {
                throwDbConnException(conn, null);
            }
            try {
                try {
                    PreparedStatement stm = JdbcUtils.prepareStatement(conn, sql, params);
                    executeNonSelect(stm);
                    if (cacheKeyKey != null) {
                        deleteFromCache(cacheKeyKey);
                    }
                    if (cacheKeySubkey != null) {
                        deleteFromCache(cacheKeySubkey);
                    }
                } finally {
                    long endTimestamp = System.currentTimeMillis();
                    JdbcLogEntry jdbcLogEntry = new JdbcLogEntry(startTimestamp, endTimestamp, sql,
                            params);
                    JdbcLogger.log(jdbcLogEntry);
                }
            } finally {
                releaseConnection(conn);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteRecords(String schemaName, String tableName, String id, String key) {
        Map<String, Object>[] records = getRecords(schemaName, tableName, id, key);
        if (records != null && records.length > 0) {
            for (Map<String, Object> record : records) {
                Object subkey = record.get(JdbcTableInfoBo.COL_SUBKEY);
                subkey = subkey != null ? subkey.toString() : "";
                deleteRecord(schemaName, tableName, id, key, subkey.toString());
            }
        }
        String cacheKey = calcCacheKey(schemaName, tableName, id, key);
        deleteFromCache(cacheKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteRecords(String schemaName, String tableName, String id) {
        Map<String, Map<String, Object>[]> entries = getRecords(schemaName, tableName, id);
        if (entries != null && entries.size() > 0) {
            for (Entry<String, Map<String, Object>[]> entry : entries.entrySet()) {
                String key = entry.getKey();
                Map<String, Object>[] records = entry.getValue();
                if (records != null && records.length > 0) {
                    for (Map<String, Object> record : records) {
                        Object subkey = record.get(JdbcTableInfoBo.COL_SUBKEY);
                        subkey = subkey != null ? subkey.toString() : "";
                        deleteRecord(schemaName, tableName, id, key, subkey.toString());
                    }
                }
                String cacheKey = calcCacheKey(schemaName, tableName, id, key);
                deleteFromCache(cacheKey);
            }
            String cacheKey = calcCacheKey(schemaName, tableName, id);
            deleteFromCache(cacheKey);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteRecord(String schemaName, String tableName, String id, String key,
            String subkey) {
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return;
        }
        String sql = tableInfo.getSqlDelete();
        try {
            long startTimestamp = System.currentTimeMillis();
            Map<String, Object> params = populateParams(id, key, subkey);
            Connection conn = getConnection();
            if (conn == null) {
                throwDbConnException(conn, null);
            }
            try {
                try {
                    PreparedStatement stm = JdbcUtils.prepareStatement(conn, sql, params);
                    executeNonSelect(stm);
                    {
                        String cacheKey = calcCacheKey(schemaName, tableName, id, key, subkey);
                        deleteFromCache(cacheKey);
                    }
                    // {
                    // String cacheKey = calcCacheKey(schemaName, tableName, id,
                    // key);
                    // deleteFromCache(cacheKey);
                    // }
                    // {
                    // String cacheKey = calcCacheKey(schemaName, tableName,
                    // id);
                    // deleteFromCache(cacheKey);
                    // }
                } finally {
                    long endTimestamp = System.currentTimeMillis();
                    JdbcLogEntry jdbcLogEntry = new JdbcLogEntry(startTimestamp, endTimestamp, sql,
                            params);
                    JdbcLogger.log(jdbcLogEntry);
                }
            } finally {
                releaseConnection(conn);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getRecord(String schemaName, String tableName, String id,
            String key, String subkey) {
        // check if table exists
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return null;
        }

        // load record
        String cacheKey = calcCacheKey(schemaName, tableName, id, key, subkey);
        Map<String, Object> result = (Map<String, Object>) getFromCache(cacheKey);
        if (result == null) {
            String sql = tableInfo.getSqlSelect();
            try {
                long startTimestamp = System.currentTimeMillis();
                Map<String, Object> params = populateParams(id, key, subkey);
                Connection conn = getConnection();
                try {
                    try {
                        PreparedStatement stm = JdbcUtils.prepareStatement(conn, sql, params);
                        List<Map<String, Object>> dbResult = executeSelect(stm);
                        result = dbResult != null && dbResult.size() > 0 ? dbResult.get(0) : null;
                        putToCache(cacheKey, result);
                    } finally {
                        long endTimestamp = System.currentTimeMillis();
                        JdbcLogEntry jdbcLogEntry = new JdbcLogEntry(startTimestamp, endTimestamp,
                                sql, params);
                        JdbcLogger.log(jdbcLogEntry);
                    }
                } finally {
                    releaseConnection(conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object>[] getRecords(String schemaName, String tableName, String id,
            String key) {
        // check if table exists
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return null;
        }

        // load subkeys
        String cacheKey = calcCacheKey(schemaName, tableName, id, key);
        String[] subkeys = (String[]) getFromCache(cacheKey);
        if (subkeys == null) {
            String sql = tableInfo.getSqlSelectSubkeys();
            try {
                long startTimestamp = System.currentTimeMillis();
                Map<String, Object> params = populateParams(id, key, null);
                Connection conn = getConnection();
                try {
                    try {
                        PreparedStatement stm = JdbcUtils.prepareStatement(conn, sql, params);
                        List<Map<String, Object>> dbResult = executeSelect(stm);
                        subkeys = new String[dbResult.size()];
                        for (int i = 0, n = dbResult.size(); i < n; i++) {
                            Object value = dbResult.get(i).get(JdbcTableInfoBo.COL_SUBKEY);
                            subkeys[i] = value.toString();
                        }
                        putToCache(cacheKey, subkeys);
                    } finally {
                        long endTimestamp = System.currentTimeMillis();
                        JdbcLogEntry jdbcLogEntry = new JdbcLogEntry(startTimestamp, endTimestamp,
                                sql, params);
                        JdbcLogger.log(jdbcLogEntry);
                    }
                } finally {
                    releaseConnection(conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // load records
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for (String subkey : subkeys) {
            Map<String, Object> record = getRecord(schemaName, tableName, id, cacheKey, subkey);
            if (record != null) {
                result.add(record);
            }
        }
        Map<String, Object>[] EMPTY_MAP_ARR = (Map<String, Object>[]) Array.newInstance(Map.class,
                0);
        return result.toArray(EMPTY_MAP_ARR);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Map<String, Object>[]> getRecords(String schemaName, String tableName,
            String id) {
        // check if table exists
        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
        if (tableInfo == null) {
            return null;
        }

        // load keys
        String cacheKey = calcCacheKey(schemaName, tableName, id);
        String[] keys = (String[]) getFromCache(cacheKey);
        if (keys == null) {
            String sql = tableInfo.getSqlSelectKeys();
            try {
                long startTimestamp = System.currentTimeMillis();
                Map<String, Object> params = populateParams(id, null, null);
                Connection conn = getConnection();
                try {
                    try {
                        PreparedStatement stm = JdbcUtils.prepareStatement(conn, sql, params);
                        List<Map<String, Object>> dbResult = executeSelect(stm);
                        keys = new String[dbResult.size()];
                        for (int i = 0, n = dbResult.size(); i < n; i++) {
                            Object value = dbResult.get(i).get(JdbcTableInfoBo.COL_KEY);
                            keys[i] = value.toString();
                        }
                        putToCache(cacheKey, keys);
                    } finally {
                        long endTimestamp = System.currentTimeMillis();
                        JdbcLogEntry jdbcLogEntry = new JdbcLogEntry(startTimestamp, endTimestamp,
                                sql, params);
                        JdbcLogger.log(jdbcLogEntry);
                    }
                } finally {
                    releaseConnection(conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // load records
        Map<String, Map<String, Object>[]> result = new HashMap<String, Map<String, Object>[]>();
        for (String key : keys) {
            Map<String, Object>[] records = getRecords(schemaName, tableName, id, key);
            if (records != null && records.length > 0) {
                result.put(key, records);
            }
        }
        return result;
    }
}
