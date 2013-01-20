package ddth.dzitdb.bo;

public interface ITableInfoVo extends ITableInfoBo {

    public ITableInfoVo populate(ITableInfoBo prototype);

    public ITableInfoVo setSchemaName(String schemaName);

    public ITableInfoVo setTableName(String tableName);

    public ITableInfoVo setTableInfoEncodedString(String jsonString);

    public ITableInfoVo addColumn(IDzitDbDao.EColumnType columnType, boolean hasIndex);
}
