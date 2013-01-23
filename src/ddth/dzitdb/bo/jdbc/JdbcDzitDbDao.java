package ddth.dzitdb.bo.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import ddth.dasp.framework.bo.jdbc.BaseJdbcBoManager;
import ddth.dasp.framework.bo.jdbc.IJdbcBoManager;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.ITableInfoBo;
import ddth.dzitdb.bo.ITableInfoDao;
import ddth.dzitdb.bo.ITableInfoVo;

public class JdbcDzitDbDao extends BaseJdbcBoManager implements IDzitDbDao {

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(String schemaName, String tableName,
            Map<IDzitDbDao.EColumnType, Boolean> columnInfo) {
        if (!tableName.startsWith(IDzitDbDao.TABLE_PREFIX)) {
            tableName = IDzitDbDao.TABLE_PREFIX + tableName;
        }

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
                params.put("tableName", schemaName + "." + tableName);
                executeStoredProcedure(sqlKey, params);
            }

            // second: add columns
            {
                String[] sqlKey = new String[] { "sql.addField" };
                Map<String, Object> params = new HashMap<String, Object>();
                params.put("tableName", schemaName + "." + tableName);
                for (Entry<IDzitDbDao.EColumnType, Boolean> entry : columnInfo.entrySet()) {
                    Boolean hasIndex = entry.getValue();
                    params.put("columnType", JdbcTableInfoBo.columnTypeToStr(entry.getKey()));
                    params.put("hasIndex", (hasIndex != null && hasIndex) ? Boolean.TRUE
                            : Boolean.FALSE);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteTable(String schemaName, String tableName) {
        if (!tableName.startsWith(IDzitDbDao.TABLE_PREFIX)) {
            tableName = IDzitDbDao.TABLE_PREFIX + tableName;
        }

        ITableInfoBo tableInfo = tableInfoDao.getTableInfo(schemaName, tableName);
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
            params.put("tableName", schemaName + "." + tableName);
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
}
