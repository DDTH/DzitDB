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

    public void deleteTable(String schemaName, String tableName);

    /*--------------------------------------------------------------------------------*/

    /**
     * Creates a new record and returns the record's id.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @param key
     * @param subkey
     * @param recordData
     * @return
     */
    public String createRecord(String schemaName, String tableName, String id, String key,
            String subkey, Map<IDzitDbDao.EColumnType, Object> recordData);

    /*--------------------------------------------------------------------------------*/

    /**
     * Deletes a record.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @param key
     * @param subkey
     */
    public void deleteRecord(String schemaName, String tableName, String id, String key,
            String subkey);

    /**
     * Deletes all records that match a key.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @param key
     */
    public void deleteRecords(String schemaName, String tableName, String id, String key);

    /**
     * Deletes all records that match an id.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     */
    public void deleteRecords(String schemaName, String tableName, String id);

    /*--------------------------------------------------------------------------------*/

    /**
     * Gets a single record.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @param key
     * @param subkey
     * @return
     */
    public Map<String, Object> getRecord(String schemaName, String tableName, String id,
            String key, String subkey);

    /**
     * Gets all subkey records of an entity.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @param key
     * @return
     */
    public Map<String, Object>[] getRecords(String schemaName, String tableName, String id,
            String key);

    /**
     * Gets all records of an entity.
     * 
     * @param schemaName
     * @param tableName
     * @param id
     * @return
     */
    public Map<String, Map<String, Object>[]> getRecords(String schemaName, String tableName,
            String id);
}
