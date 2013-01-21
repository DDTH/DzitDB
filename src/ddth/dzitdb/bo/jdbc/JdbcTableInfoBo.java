package ddth.dzitdb.bo.jdbc;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import ddth.dasp.common.utils.JsonUtils;
import ddth.dasp.framework.bo.FieldMapping;
import ddth.dasp.framework.bo.jdbc.BaseJdbcBo;
import ddth.dasp.framework.utils.DPathUtils;
import ddth.dzitdb.bo.IDzitDbDao;
import ddth.dzitdb.bo.ITableInfoBo;
import ddth.dzitdb.bo.ITableInfoVo;

public class JdbcTableInfoBo extends BaseJdbcBo implements ITableInfoVo {

    // private final static String COL_ID = "eid";
    // private final static String COL_KEY = "ekey";
    // private final static String COL_SUBKEY = "esubkey";
    // private final static String COL_TYPE = "etype";

    private final static String TEMPLATE_WHERE = "eid=@{eid} AND ekey=@{ekey} AND esubey=@{esubkey}";
    private final static String TEMPLATE_SELECT = "SELECT eid,ekey,esubkey,etype${column_list} FROM ${table_name} WHERE "
            + TEMPLATE_WHERE;
    private final static String TEMPLATE_INSERT = "INSERT INTO ${table_name} (eid,ekey,esubkey,etype${column_list}) VALUES (@{eid},@{ekey},@{esubkey},@{etype}${value_list})";
    private final static String TEMPLATE_DELETE = "DELETE FROM ${table_name} WHERE "
            + TEMPLATE_WHERE;;
    private final static String TEMPLATE_UPDATE = "UPDATE ${table_name} SET etype=@{etype},${column_value_list} WHERE "
            + TEMPLATE_WHERE;

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

    // type: String
    public final static String KEY_SQL_UPDATE = "sqls.update";

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
            columnList.append(",").append(IDzitDbDao.COL_PREFIX).append(colName);
            valueList.append(",@{").append(IDzitDbDao.COL_PREFIX).append(colName).append("}");
            columnValueList.append(",").append(IDzitDbDao.COL_PREFIX).append(colName).append("=")
                    .append("=@{").append(IDzitDbDao.COL_PREFIX).append(colName).append("}");
        }
        String sqlDelete = TEMPLATE_DELETE.replace("${table_name}", absTableName);
        String sqlInsert = TEMPLATE_INSERT.replace("${table_name}", absTableName)
                .replace("${column_list}", columnList.toString())
                .replace("${value_list}", valueList.toString());
        String sqlSelect = TEMPLATE_SELECT.replace("${table_name}", absTableName).replace(
                "${column_list}", columnList.toString());
        String sqlUpdate = TEMPLATE_UPDATE.replace("${table_name}", absTableName).replace(
                "${column_value_list}", columnValueList.toString());
        DPathUtils.setSetValue(tableInfo, KEY_SQL_DELETE, sqlDelete);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_INSERT, sqlInsert);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_SELECT, sqlSelect);
        DPathUtils.setSetValue(tableInfo, KEY_SQL_UPDATE, sqlUpdate);

        this.jsonTableInfo = JsonUtils.toJson(this.tableInfo);
    }

    public static String columnTypeToStr(IDzitDbDao.EColumnType columnType) {
        switch (columnType) {
        case BINARY:
            return "binary";
        case DATETIME:
            return "datetime";
        case INT:
            return "int";
        case LONG_STRING:
            return "long_string";
        case MONEY:
            return "money";
        case REAL:
            return "real";
        case SHORT_STRING:
            return "short_string";
        default:
            throw new IllegalArgumentException("Not supported column type: " + columnType);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasColumn(IDzitDbDao.EColumnType columnType) {
        return DPathUtils.getValue(tableInfo,
                MessageFormat.format(KEY_COLUMN_INFO, columnTypeToStr(columnType))) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasIndex(IDzitDbDao.EColumnType columnType) {
        Object obj = DPathUtils.getValue(tableInfo,
                MessageFormat.format(KEY_COLUMN_INFO_INDEX, columnTypeToStr(columnType)));
        return obj instanceof Boolean && ((Boolean) obj);
    }

    public ITableInfoVo addColumn(IDzitDbDao.EColumnType columnType, boolean hasIndex) {
        if (hasColumn(columnType)) {
            throw new IllegalArgumentException("Column [" + columnType + "] already exists!");
        }
        String columnTypeStr = columnTypeToStr(columnType);
        DPathUtils.setSetValue(tableInfo, MessageFormat.format(KEY_COLUMN_INFO, columnTypeStr),
                new HashMap<String, Object>());
        DPathUtils.setSetValue(tableInfo, MessageFormat
                .format(KEY_COLUMN_INFO_INDEX, columnTypeStr), hasIndex ? Boolean.TRUE
                : Boolean.FALSE);
        refreshTableInfo();
        return this;
    }
}
