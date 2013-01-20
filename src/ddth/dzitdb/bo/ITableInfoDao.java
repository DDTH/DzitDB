package ddth.dzitdb.bo;

public interface ITableInfoDao {

    public final static String FIELD_INFO_SCHEMA_NAME = "dbschema";
    public final static String FIELD_INFO_TABLE_NAME = "dbtable";
    public final static String FIELD_INFO_TABLE_INFO = "dbtable_info";

    public ITableInfoVo createTableInfoVoObj();

    public ITableInfoVo createTableInfoVoObj(ITableInfoBo prototype);

    /*--------------------------------------------------*/

    public ITableInfoBo createTableInfo(ITableInfoBo tableInfo);

    public ITableInfoBo getTableInfo(String schemaName, String tableName);

    public ITableInfoBo updateTableInfo(ITableInfoBo tableInfo);
}
