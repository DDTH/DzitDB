package ddth.dzitdb.bo;

import java.util.Map;

public interface IDzitDbDao {

    public final static String TABLE_PREFIX = "dtbl_";
    public final static String COL_PREFIX = "value_";

    public static enum EColumnType {
        INT, REAL, MONEY, DATETIME, SHORT_STRING, LONG_STRING, BINARY
    }

    public boolean tableExists(String schemaName, String tableName);

    public void createTable(String schemaName, String tableName,
            Map<IDzitDbDao.EColumnType, Boolean> columnInfo);
}
