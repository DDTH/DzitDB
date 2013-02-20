package ddth.dzitdb.bo;

public interface ITableInfoBo {

    public final static String DEFAULT_JSON_TABLE_INFO_ENCODED_STRING = "{\"columns\":{}, \"sqls\":{}}";

    public String getSchemaName();

    public String getTableName();

    public String getTableInfoEncodedString();

    public String[] getColumnNames();

    public boolean hasColumn(IDzitDbDao.EColumnType columnType);

    public boolean hasIndex(IDzitDbDao.EColumnType columnType);

    public String getSqlDelete();

    public String getSqlInsert();

    public String getSqlSelect();

    public String getSqlSelectKeys();

    public String getSqlSelectSubkeys();

    public String getSqlUpdate();
}
