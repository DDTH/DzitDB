package ddth.dzitdb.bo.jdbc;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;

import ddth.dasp.common.utils.JsonUtils;
import ddth.dasp.framework.bo.FieldMapping;
import ddth.dasp.framework.bo.jdbc.BaseJdbcBo;
import ddth.dasp.framework.utils.DPathUtils;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.ITableInfoBo;
import ddth.dzitdb.bo.ITableInfoVo;

public class JdbcTableInfoBo extends BaseJdbcBo implements ITableInfoVo {

    public final static String COL_ID = "eid";
    public final static String COL_KEY = "ekey";
    public final static String COL_SUBKEY = "esubkey";

    private final static String TEMPLATE_WHERE_FULL = "eid=@{eid} AND ekey=@{ekey} AND esubey=@{esubkey}";
    private final static String TEMPLATE_WHERE_SUBKEY = "eid=@{eid} AND ekey=@{ekey}";
    private final static String TEMPLATE_WHERE_KEY = "eid=@{eid}";

    private final static String TEMPLATE_SELECT = "SELECT eid,ekey,esubkey${column_list} FROM ${table_name} WHERE "
            + TEMPLATE_WHERE_FULL;
    private final static String TEMPLATE_SELECT_SUBKEYS = "SELECT esubkey FROM ${table_name} ORDER BY esubkey WHERE "
            + TEMPLATE_WHERE_SUBKEY;
    private final static String TEMPLATE_SELECT_KEYS = "SELECT ekey FROM ${table_name} WHERE "
            + TEMPLATE_WHERE_KEY;

    private final static String TEMPLATE_INSERT = "INSERT INTO ${table_name} (eid,ekey,esubkey${column_list}) VALUES (@{eid},@{ekey},@{esubkey}${value_list})";
    private final static String TEMPLATE_DELETE = "DELETE FROM ${table_name} WHERE "
            + TEMPLATE_WHERE_FULL;;
    private final static String TEMPLATE_UPDATE = "UPDATE ${table_name} SET ${column_value_list} WHERE "
            + TEMPLATE_WHERE_FULL;

    // type: Map
    public final static String KEY_COLUMNS = "columns";

    // type: Map
    public final static String KEY_COLUMN_INFO = "columns.{0}";

    // type: Boolean
    public final static String KEY_COLUMN_INFO_INDEX = "columns.{0}.index";

    // type: String
    public final static String KEY_SQL_DELETE = "sqls.delete";

    // type: String
    public final static String KEY_SQL_INSERT = "sqls.insert";

    // type: String
    public final static String KEY_SQL_SELECT = "sqls.select";
    public final static String KEY_SQL_SELECT_KEYS = "sqls.selectKeys";
    public final static String KEY_SQL_SELECT_SUBKEYS = "sqls.selectSubkeys";

    // type: String
    public final static String KEY_SQL_UPDATE = "sqls.update";

    public final static String COL_TYPE_BINARY = "binary";
    public final static String COL_TYPE_DATETIME = "datetime";
    public final static String COL_TYPE_INT = "int";
    public final static String COL_TYPE_LONG_STRING = "long_string";
    public final static String COL_TYPE_MONEY = "money";
    public final static String COL_TYPE_REAL = "real";
    public final static String COL_TYPE_SHORT_STRING = "short_string";

    public final static String COL_NAME_BINARY = IDzitDbDao.COL_PREFIX + COL_TYPE_BINARY;
    public final static String COL_NAME_DATETIME = IDzitDbDao.COL_PREFIX + COL_TYPE_DATETIME;
    public final static String COL_NAME_INT = IDzitDbDao.COL_PREFIX + COL_TYPE_INT;
    public final static String COL_NAME_LONG_STRING = IDzitDbDao.COL_PREFIX + COL_TYPE_LONG_STRING;
    public final static String COL_NAME_MONEY = IDzitDbDao.COL_PREFIX + COL_TYPE_MONEY;
    public final static String COL_NAME_REAL = IDzitDbDao.COL_PREFIX + COL_TYPE_REAL;
    public final static String COL_NAME_SHORT_STRING = IDzitDbDao.COL_PREFIX
            + COL_TYPE_SHORT_STRING;

    public final static String COL_DBSCHEMA = "dbschema";
    public final static String COL_DBTABLE = "dbtable";
    public final static String COL_DBTABLE_INFO = "dbtable_info";

    private String schemaName, tableName;
    private String jsonTableInfo;
    private Map<String, Object> tableInfo = new HashMap<String, Object>();

    public JdbcTableInfoBo() {
        // setTableInfoEncodedString(ITableInfoBo.DEFAULT_JSON_TABLE_INFO_ENCODED_STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ITableInfoVo populate(ITableInfoBo prototype) {
        setSchemaName(prototype.getSchemaName());
        setTableName(prototype.getTableName());
        setTableInfoEncodedString(prototype.getTableInfoEncodedString());
        return this;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @FieldMapping(field = COL_DBSCHEMA, type = String.class)
    @Override
    public JdbcTableInfoBo setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @FieldMapping(field = COL_DBTABLE, type = String.class)
    @Override
    public JdbcTableInfoBo setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public String getTableInfoEncodedString() {
        return jsonTableInfo;
    }

    protected String getAbsoluteTableName() {
        return schemaName + "." + tableName;
    }

    @SuppressWarnings("unchecked")
    @FieldMapping(field = COL_DBTABLE_INFO, type = String.class)
    @Override
    public JdbcTableInfoBo setTableInfoEncodedString(String jsonString) {
        this.jsonTableInfo = jsonString;
        try {
            this.tableInfo = JsonUtils.fromJson(this.jsonTableInfo, Map.class);
        } catch (Exception e) {
            this.jsonTableInfo = DEFAULT_JSON_TABLE_INFO_ENCODED_STRING;
            this.tableInfo = JsonUtils.fromJson(this.jsonTableInfo, Map.class);
        }
        refreshTableInfo();
        return this;
    }

    @SuppressWarnings("unchecked")
    private void refreshTableInfo() {
        Object obj = DPathUtils.getValue(tableInfo, KEY_COLUMNS);
        if (!(obj instanceof Map)) {
            obj = new HashMap<String, Object>();
            DPathUtils.setSetValue(tableInfo, KEY_COLUMNS, obj);
        }
        String absTableName = getAbsoluteTableName();
        StringBuilder columnList = new StringBuilder();
        StringBuilder valueList = new StringBuilder();
        StringBuilder columnValueList = new StringBuilder();
        Map<String, Object> columnInfo = (Map<String, Object>) obj;
        for (Entry<String, Object> entry : columnInfo.entrySet()) {
            String colName = entry.getKey();
            columnList.append(",").append(colName);
            valueList.append(",@{").append(colName).append("}");
            columnValueList.append(",").append(colName).append("=").append("=@{").append(colName)
                    .append("}");
        }
        String sqlDelete = TEMPLATE_DELETE.replace("${table_name}", absTableName);
        String sqlInsert = TEMPLATE_INSERT.replace("${table_name}", absTableName)
                .replace("${column_list}", columnList.toString())
                .replace("${value_list}", valueList.toString());
        String sqlSelect = TEMPLATE_SELECT.replace("${table_name}", absTableName).replace(
                "${column_list}", columnList.toString());
        String sqlSelectKeys = TEMPLATE_SELECT_KEYS.replace("${table_name}", absTableName).replace(
                "${column_list}", columnList.toString());
        String sqlSelectSubkeys = TEMPLATE_SELECT_SUBKEYS.replace("${table_name}", absTableName)
                .replace("${column_list}", columnList.toString());
        String sqlUpdate = TEMPLATE_UPDATE.replace("${table_name}", absTableName).replace(
                "${column_value_list}", columnValueList.toString());
        DPathUtils.setSetValue(tableInfo, KEY_SQL_DELETE, sqlDelete);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_INSERT, sqlInsert);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_SELECT, sqlSelect);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_SELECT_KEYS, sqlSelectKeys);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_SELECT_SUBKEYS, sqlSelectSubkeys);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_UPDATE, sqlUpdate);

        this.jsonTableInfo = JsonUtils.toJson(this.tableInfo);
    }

    public static String columnTypeToColumnName(IDzitDbDao.EColumnType columnType) {
        switch (columnType) {
        case BINARY:
            return COL_NAME_BINARY;
        case DATETIME:
            return COL_NAME_DATETIME;
        case INT:
            return COL_NAME_INT;
        case LONG_STRING:
            return COL_NAME_LONG_STRING;
        case MONEY:
            return COL_NAME_MONEY;
        case REAL:
            return COL_NAME_REAL;
        case SHORT_STRING:
            return COL_NAME_SHORT_STRING;
        default:
            throw new IllegalArgumentException("Not supported column type: " + columnType);
        }
    }

    public static String columnTypeToStr(IDzitDbDao.EColumnType columnType) {
        switch (columnType) {
        case BINARY:
            return COL_TYPE_BINARY;
        case DATETIME:
            return COL_TYPE_DATETIME;
        case INT:
            return COL_TYPE_INT;
        case LONG_STRING:
            return COL_TYPE_LONG_STRING;
        case MONEY:
            return COL_TYPE_MONEY;
        case REAL:
            return COL_TYPE_REAL;
        case SHORT_STRING:
            return COL_TYPE_SHORT_STRING;
        default:
            throw new IllegalArgumentException("Not supported column type: " + columnType);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public String[] getColumnNames() {
        Map<String, Object> columns = (Map<String, Object>) DPathUtils.getValue(tableInfo,
                KEY_COLUMNS);
        List<String> result = new ArrayList<String>();
        for (Entry<String, Object> entry : columns.entrySet()) {
            result.add(entry.getKey());
        }
        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasColumn(IDzitDbDao.EColumnType columnType) {
        return DPathUtils.getValue(tableInfo,
                MessageFormat.format(KEY_COLUMN_INFO, columnTypeToColumnName(columnType))) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasIndex(IDzitDbDao.EColumnType columnType) {
        Object obj = DPathUtils.getValue(tableInfo,
                MessageFormat.format(KEY_COLUMN_INFO_INDEX, columnTypeToColumnName(columnType)));
        return obj instanceof Boolean && ((Boolean) obj);
    }

    public ITableInfoVo addColumn(IDzitDbDao.EColumnType columnType, boolean hasIndex) {
        if (hasColumn(columnType)) {
            throw new IllegalArgumentException("Column [" + columnType + "] already exists!");
        }
        String columnTypeStr = columnTypeToColumnName(columnType);
        DPathUtils.setSetValue(tableInfo, MessageFormat.format(KEY_COLUMN_INFO, columnTypeStr),
                new HashMap<String, Object>());
        DPathUtils.setSetValue(tableInfo, MessageFormat
                .format(KEY_COLUMN_INFO_INDEX, columnTypeStr), hasIndex ? Boolean.TRUE
                : Boolean.FALSE);
        refreshTableInfo();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSqlDelete() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_DELETE);
        return value != null ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSqlInsert() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_INSERT);
        return value != null ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSqlSelect() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_SELECT);
        return value != null ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSqlSelectKeys() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_SELECT_KEYS);
        return value != null ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSqlSelectSubkeys() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_SELECT_SUBKEYS);
        return value != null ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    public String getSqlUpdate() {
        Object value = DPathUtils.getValue(tableInfo, KEY_SQL_UPDATE);
        return value != null ? value.toString() : null;
    }
}
