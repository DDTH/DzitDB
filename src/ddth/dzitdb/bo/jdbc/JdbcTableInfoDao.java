package ddth.dzitdb.bo.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ddth.dasp.framework.bo.jdbc.BaseJdbcBoManager;
import ddth.dzitdb.bo.ITableInfoBo;
import ddth.dzitdb.bo.ITableInfoDao;

public class JdbcTableInfoDao extends BaseJdbcBoManager implements ITableInfoDao {

    private String tblTableInfo = "dzitdb_info";

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcTableInfoBo createTableInfoVoObj() {
        return createBusinessObject(JdbcTableInfoBo.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcTableInfoBo createTableInfoVoObj(ITableInfoBo prototype) {
        JdbcTableInfoBo obj = (JdbcTableInfoBo) createTableInfoVoObj().populate(prototype);
        return obj;
    }

    protected String createCacheKey(ITableInfoBo bo) {
        return createCacheKeyTableInfo(bo.getSchemaName(), bo.getTableName());
    }

    protected String createCacheKeyTableInfo(String schemaName, String tableName) {
        return schemaName + "." + tableName;
    }

    /*--------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public ITableInfoBo createTableInfo(ITableInfoBo tableInfo) {
        String cacheKey = createCacheKey(tableInfo);
        String[] sqlKey = new String[] { "sql.createTableInfo", tblTableInfo };
        Map<String, Object> sqlParams = new HashMap<String, Object>();
        sqlParams.put(JdbcTableInfoBo.COL_DBSCHEMA, tableInfo.getSchemaName());
        sqlParams.put(JdbcTableInfoBo.COL_DBTABLE, tableInfo.getTableName());
        sqlParams.put(JdbcTableInfoBo.COL_DBTABLE_INFO, tableInfo.getTableInfoEncodedString());
        try {
            executeNonSelect(sqlKey, sqlParams);
            deleteFromCache(cacheKey);
            return tableInfo;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ITableInfoBo getTableInfo(String schemaName, String tableName) {
        String cacheKey = createCacheKeyTableInfo(schemaName, tableName);
        String[] sqlKey = new String[] { "sql.getTableInfo", tblTableInfo };
        Map<String, Object> sqlParams = new HashMap<String, Object>();
        sqlParams.put(JdbcTableInfoBo.COL_DBSCHEMA, schemaName);
        sqlParams.put(JdbcTableInfoBo.COL_DBTABLE, tableName);
        try {
            JdbcTableInfoBo[] result = executeSelect(sqlKey, sqlParams, JdbcTableInfoBo.class,
                    cacheKey);
            return result != null && result.length > 0 ? result[0] : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ITableInfoBo updateTableInfo(ITableInfoBo tableInfo) {
        String cacheKey = createCacheKey(tableInfo);
        String[] sqlKey = new String[] { "sql.getTableInfo", tblTableInfo };
        Map<String, Object> sqlParams = new HashMap<String, Object>();
        sqlParams.put(JdbcTableInfoBo.COL_DBSCHEMA, tableInfo.getSchemaName());
        sqlParams.put(JdbcTableInfoBo.COL_DBTABLE, tableInfo.getTableName());
        sqlParams.put(JdbcTableInfoBo.COL_DBTABLE_INFO, tableInfo.getTableInfoEncodedString());
        try {
            executeNonSelect(sqlKey, sqlParams);
            deleteFromCache(cacheKey);
            return tableInfo;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
